package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/sessions"
	"github.com/hlts2/gocache"
	"github.com/labstack/echo-contrib/session"
	"github.com/labstack/echo/v4"
	"golang.org/x/crypto/bcrypt"
)

const (
	defaultSessionIDKey      = "SESSIONID"
	defaultSessionExpiresKey = "EXPIRES"
	defaultUserIDKey         = "USERID"
	defaultUsernameKey       = "USERNAME"
	bcryptDefaultCost        = bcrypt.MinCost
)

var fallbackImage = "../img/NoImage.jpg"

type UserModel struct {
	ID             int64  `db:"id"`
	Name           string `db:"name"`
	DisplayName    string `db:"display_name"`
	Description    string `db:"description"`
	HashedPassword string `db:"password"`
	DarkMode       bool   `db:"dark_mode"`
	Reactions      int64  `db:"reactions"`
	Tips           int64  `db:"tips"`
	LiveComments   int64  `db:"live_comments"`
	IconHash       []byte `db:"icon_hash"`
}

type User struct {
	ID          int64  `json:"id"`
	Name        string `json:"name"`
	DisplayName string `json:"display_name,omitempty"`
	Description string `json:"description,omitempty"`
	Theme       Theme  `json:"theme,omitempty"`
	IconHash    string `json:"icon_hash,omitempty"`
}

type Theme struct {
	ID       int64 `json:"id"`
	DarkMode bool  `json:"dark_mode"`
}

type ThemeModel struct {
	ID       int64 `db:"id"`
	UserID   int64 `db:"user_id"`
	DarkMode bool  `db:"dark_mode"`
}

type PostUserRequest struct {
	Name        string `json:"name"`
	DisplayName string `json:"display_name"`
	Description string `json:"description"`
	// Password is non-hashed password.
	Password string               `json:"password"`
	Theme    PostUserRequestTheme `json:"theme"`
}

type PostUserRequestTheme struct {
	DarkMode bool `json:"dark_mode"`
}

type LoginRequest struct {
	Username string `json:"username"`
	// Password is non-hashed password.
	Password string `json:"password"`
}

type PostIconRequest struct {
	Image []byte `json:"image"`
}

type PostIconResponse struct {
	ID int64 `json:"id"`
}

var iconCache = gocache.New(gocache.WithExpireAt(1500 * time.Millisecond))
var userCache = gocache.New(gocache.WithExpireAt(60 * time.Minute))

func getUserOnlyCache(userId int64) *UserModel {
	if user, found := userCache.Get(fmt.Sprintf("id:%d", userId)); found {
		u := user.(*UserModel)
		iconHash, valid := iconCache.Get(u.Name)
		if valid {
			u.IconHash = iconHash.([]byte)
			return u
		}
	}
	return nil
}

func getUserWithCache(ctx context.Context, userId int64) (*UserModel, error) {
	if user, found := userCache.Get(fmt.Sprintf("id:%d", userId)); found {
		u := user.(*UserModel)
		iconHash, valid := iconCache.Get(u.Name)
		if valid {
			u.IconHash = iconHash.([]byte)
			return u, nil
		}
	}

	var userModel UserModel
	if err := dbConn.GetContext(ctx, &userModel, "SELECT `id`,`name`,`display_name`,`description`,`password`,`dark_mode`,`icon_hash` FROM users WHERE id = ?", userId); err != nil {
		return nil, err
	}

	userCache.Set(fmt.Sprintf("id:%d", userId), &userModel)
	userCache.Set(fmt.Sprintf("name:%s", userModel.Name), &userModel)
	iconCache.Set(userModel.Name, userModel.IconHash)
	return &userModel, nil
}

func getUserByName(ctx context.Context, userName string) (*UserModel, error) {
	if user, found := userCache.Get(fmt.Sprintf("name:%s", userName)); found {
		u := user.(*UserModel)
		iconHash, valid := iconCache.Get(u.Name)
		if valid {
			u.IconHash = iconHash.([]byte)
			return u, nil
		}
	}

	var userModel UserModel
	if err := dbConn.GetContext(ctx, &userModel, "SELECT `id`,`name`,`display_name`,`description`,`password`,`dark_mode`,`icon_hash` FROM users WHERE name = ?", userName); err != nil {
		return nil, err
	}

	userCache.Set(fmt.Sprintf("id:%d", userModel.ID), &userModel)
	userCache.Set(fmt.Sprintf("name:%s", userModel.Name), &userModel)
	iconCache.Set(userModel.Name, userModel.IconHash)
	return &userModel, nil
}

func getIconHandler(c echo.Context) error {
	ctx := c.Request().Context()

	iconHash := c.Request().Header.Get("If-None-Match")
	c.Logger().Print(iconHash)

	username := c.Param("username")

	if iconHash != "" {
		cacheIconCacheHash, found := iconCache.Get(username)
		if found && iconHash == fmt.Sprintf("\"%x\"", cacheIconCacheHash.([]byte)) {
			return c.NoContent(http.StatusNotModified)
		}
	}
	user, err := getUserByName(ctx, username)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return echo.NewHTTPError(http.StatusNotFound, "not found user that has the given username")
		}
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to get user: "+err.Error())
	}

	if iconHash != "" {
		if fmt.Sprintf("\"%x\"", user.IconHash) == iconHash {
			return c.NoContent(http.StatusNotModified)
		}
	}

	var image struct {
		Image []byte `db:"image"`
	}
	if err := dbConn.GetContext(ctx, &image, "SELECT image FROM icons WHERE user_id = ?", user.ID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return c.File(fallbackImage)
		} else {
			return echo.NewHTTPError(http.StatusInternalServerError, "failed to get user icon: "+err.Error())
		}
	}
	return c.Blob(http.StatusOK, "image/jpeg", image.Image)
}

func postIconHandler(c echo.Context) error {
	ctx := c.Request().Context()

	if err := verifyUserSession(c); err != nil {
		// echo.NewHTTPErrorが返っているのでそのまま出力
		return err
	}

	// error already checked
	sess, _ := session.Get(defaultSessionIDKey, c)
	// existence already checked
	userID := sess.Values[defaultUserIDKey].(int64)

	var req *PostIconRequest
	if err := json.NewDecoder(c.Request().Body).Decode(&req); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "failed to decode the request body as json")
	}

	// imageのsha256を計算
	hash := sha256.New()
	// hash doesn't returns error
	_, _ = hash.Write(req.Image)
	iconHash := hash.Sum(nil)

	tx, err := dbConn.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to start tx: %w", err)
	}
	defer tx.Rollback()

	rs, err := tx.ExecContext(ctx, "INSERT INTO icons (user_id, image) VALUES (?, ?) as new_row ON DUPLICATE KEY UPDATE image = new_row.image", userID, req.Image)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to insert new user icon: "+err.Error())
	}

	if _, err := tx.ExecContext(ctx, "UPDATE users SET icon_hash = ? WHERE id = ?", iconHash, userID); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to insert new user icon: "+err.Error())
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("tx error: %w", err)
	}

	iconID, err := rs.LastInsertId()
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to get last inserted icon id: "+err.Error())
	}

	return c.JSON(http.StatusCreated, &PostIconResponse{
		ID: iconID,
	})
}

func getMeHandler(c echo.Context) error {
	ctx := c.Request().Context()

	if err := verifyUserSession(c); err != nil {
		// echo.NewHTTPErrorが返っているのでそのまま出力
		return err
	}

	// error already checked
	sess, _ := session.Get(defaultSessionIDKey, c)
	// existence already checked
	userID := sess.Values[defaultUserIDKey].(int64)

	userModel, err := getUserWithCache(ctx, userID)
	if errors.Is(err, sql.ErrNoRows) {
		return echo.NewHTTPError(http.StatusNotFound, "not found user that has the userid in session")
	}
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to get user: "+err.Error())
	}

	user, err := fillUserResponse(ctx, userModel)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to fill user: "+err.Error())
	}

	return c.JSON(http.StatusOK, user)
}

// ユーザ登録API
// POST /api/register
func registerHandler(c echo.Context) error {
	ctx := c.Request().Context()
	defer c.Request().Body.Close()

	req := PostUserRequest{}
	if err := json.NewDecoder(c.Request().Body).Decode(&req); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "failed to decode the request body as json")
	}

	if req.Name == "pipe" {
		return echo.NewHTTPError(http.StatusBadRequest, "the username 'pipe' is reserved")
	}

	hashedPassword, err := bcrypt.GenerateFromPassword([]byte(req.Password), bcryptDefaultCost)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to generate hashed password: "+err.Error())
	}

	tx, err := dbConn.BeginTxx(ctx, nil)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to begin transaction: "+err.Error())
	}
	defer tx.Rollback()

	userModel := UserModel{
		Name:           req.Name,
		DisplayName:    req.DisplayName,
		Description:    req.Description,
		HashedPassword: string(hashedPassword),
		DarkMode:       req.Theme.DarkMode,
		IconHash:       []byte{217, 248, 41, 78, 157, 137, 95, 129, 206, 98, 231, 61, 199, 213, 223, 248, 98, 164, 250, 64, 189, 78, 15, 236, 245, 63, 117, 38, 168, 237, 202, 192},
	}

	result, err := tx.NamedExecContext(ctx, "INSERT INTO users (name, display_name, description, password, dark_mode) VALUES(:name, :display_name, :description, :password, :dark_mode)", userModel)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to insert user: "+err.Error())
	}

	userID, err := result.LastInsertId()
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to get last inserted user id: "+err.Error())
	}

	userModel.ID = userID

	themeModel := ThemeModel{
		UserID:   userID,
		DarkMode: req.Theme.DarkMode,
	}
	if _, err := tx.NamedExecContext(ctx, "INSERT INTO themes (user_id, dark_mode) VALUES(:user_id, :dark_mode)", themeModel); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to insert user theme: "+err.Error())
	}

	//if out, err := exec.Command("pdnsutil", "add-record", "u.isucon.dev", req.Name, "A", "0", powerDNSSubdomainAddress).CombinedOutput(); err != nil {
	//	return echo.NewHTTPError(http.StatusInternalServerError, string(out)+": "+err.Error())
	//}

	// send to http request to isudns
	type RecordCreateParam struct {
		Username string `json:"username"`
	}
	param := RecordCreateParam{
		Username: req.Name,
	}
	b, err := json.Marshal(param)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to marshal json: "+err.Error())
	}

	client := &http.Client{}
	reqIsuDNS, err := http.NewRequestWithContext(ctx, "POST", fmt.Sprintf("http://%s:8082/api/record", isuDNSServerAddress), bytes.NewBuffer(b))
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to create request: "+err.Error())
	}
	resp, err := client.Do(reqIsuDNS)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to send request: "+err.Error())
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		return echo.NewHTTPError(http.StatusInternalServerError, "invalid response from isudns: %s", resp.Body)
	}

	user, err := fillUserResponse(ctx, &userModel)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to fill user: "+err.Error())
	}

	if err := tx.Commit(); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to commit: "+err.Error())
	}

	return c.JSON(http.StatusCreated, user)
}

// ユーザログインAPI
// POST /api/login
func loginHandler(c echo.Context) error {
	ctx := c.Request().Context()
	defer c.Request().Body.Close()

	req := LoginRequest{}
	if err := json.NewDecoder(c.Request().Body).Decode(&req); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "failed to decode the request body as json")
	}

	tx, err := dbConn.BeginTxx(ctx, nil)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to begin transaction: "+err.Error())
	}
	defer tx.Rollback()

	// usernameはUNIQUEなので、whereで一意に特定できる
	userModel, err := getUserByName(ctx, req.Username)
	if errors.Is(err, sql.ErrNoRows) {
		return echo.NewHTTPError(http.StatusUnauthorized, "invalid username or password")
	}
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to get user: "+err.Error())
	}

	if err := tx.Commit(); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to commit: "+err.Error())
	}

	err = bcrypt.CompareHashAndPassword([]byte(userModel.HashedPassword), []byte(req.Password))
	if err == bcrypt.ErrMismatchedHashAndPassword {
		return echo.NewHTTPError(http.StatusUnauthorized, "invalid username or password")
	}
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to compare hash and password: "+err.Error())
	}

	sessionEndAt := time.Now().Add(1 * time.Hour)

	sessionID := uuid.NewString()

	sess, err := session.Get(defaultSessionIDKey, c)
	if err != nil {
		return echo.NewHTTPError(http.StatusUnauthorized, "failed to get session")
	}

	sess.Options = &sessions.Options{
		Domain: "u.isucon.dev",
		MaxAge: int(60000),
		Path:   "/",
	}
	sess.Values[defaultSessionIDKey] = sessionID
	sess.Values[defaultUserIDKey] = userModel.ID
	sess.Values[defaultUsernameKey] = userModel.Name
	sess.Values[defaultSessionExpiresKey] = sessionEndAt.Unix()

	if err := sess.Save(c.Request(), c.Response()); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to save session: "+err.Error())
	}

	return c.NoContent(http.StatusOK)
}

// ユーザ詳細API
// GET /api/user/:username
func getUserHandler(c echo.Context) error {
	ctx := c.Request().Context()
	if err := verifyUserSession(c); err != nil {
		// echo.NewHTTPErrorが返っているのでそのまま出力
		return err
	}

	username := c.Param("username")

	tx, err := dbConn.BeginTxx(ctx, nil)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to begin transaction: "+err.Error())
	}
	defer tx.Rollback()

	userModel, err := getUserByName(ctx, username)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return echo.NewHTTPError(http.StatusNotFound, "not found user that has the given username")
		}
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to get user: "+err.Error())
	}

	user, err := fillUserResponse(ctx, userModel)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to fill user: "+err.Error())
	}

	if err := tx.Commit(); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to commit: "+err.Error())
	}

	return c.JSON(http.StatusOK, user)
}

func verifyUserSession(c echo.Context) error {
	sess, err := session.Get(defaultSessionIDKey, c)
	if err != nil {
		return echo.NewHTTPError(http.StatusUnauthorized, "failed to get session")
	}

	sessionExpires, ok := sess.Values[defaultSessionExpiresKey]
	if !ok {
		return echo.NewHTTPError(http.StatusForbidden, "failed to get EXPIRES value from session")
	}

	_, ok = sess.Values[defaultUserIDKey].(int64)
	if !ok {
		return echo.NewHTTPError(http.StatusUnauthorized, "failed to get USERID value from session")
	}

	now := time.Now()
	if now.Unix() > sessionExpires.(int64) {
		return echo.NewHTTPError(http.StatusUnauthorized, "session has expired")
	}

	return nil
}

func fillUserResponse(ctx context.Context, userModel *UserModel) (User, error) {
	user := User{
		ID:          userModel.ID,
		Name:        userModel.Name,
		DisplayName: userModel.DisplayName,
		Description: userModel.Description,
		Theme: Theme{
			ID:       userModel.ID,
			DarkMode: userModel.DarkMode,
		},
		IconHash: fmt.Sprintf("%x", userModel.IconHash),
	}

	return user, nil
}
