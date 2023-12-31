package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/labstack/echo-contrib/session"
	"github.com/labstack/echo/v4"
)

type ReactionModel struct {
	ID           int64  `db:"id"`
	EmojiName    string `db:"emoji_name"`
	UserID       int64  `db:"user_id"`
	LivestreamID int64  `db:"livestream_id"`
	CreatedAt    int64  `db:"created_at"`
}

type Reaction struct {
	ID         int64      `json:"id"`
	EmojiName  string     `json:"emoji_name"`
	User       User       `json:"user"`
	Livestream Livestream `json:"livestream"`
	CreatedAt  int64      `json:"created_at"`
}

type PostReactionRequest struct {
	EmojiName string `json:"emoji_name"`
}

// Live Streamのリアクションを指定した件数取得する
func getReactionsHandler(c echo.Context) error {
	ctx := c.Request().Context()

	if err := verifyUserSession(c); err != nil {
		// echo.NewHTTPErrorが返っているのでそのまま出力
		return err
	}

	livestreamID, err := strconv.Atoi(c.Param("livestream_id"))
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "livestream_id in path must be integer")
	}

	tx, err := dbConn.BeginTxx(ctx, nil)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to begin transaction: "+err.Error())
	}
	defer tx.Rollback()

	query := "SELECT * FROM reactions WHERE livestream_id = ? ORDER BY created_at DESC"
	if c.QueryParam("limit") != "" {
		limit, err := strconv.Atoi(c.QueryParam("limit"))
		if err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, "limit query parameter must be integer")
		}
		query += fmt.Sprintf(" LIMIT %d", limit)
	}

	reactionModels := []ReactionModel{}
	if err := tx.SelectContext(ctx, &reactionModels, query, livestreamID); err != nil {
		return echo.NewHTTPError(http.StatusNotFound, "failed to get reactions")
	}
	userIds := make([]int64, len(reactionModels))
	for i, model := range reactionModels {
		userIds[i] = model.UserID
	}
	reactionUsers, err := getUsersWithCache(ctx, tx, userIds)
	if err != nil {
		return fmt.Errorf("invalid user: %w", err)
	}
	livestreamModel := LivestreamModel{}
	err = tx.GetContext(ctx, &livestreamModel, "SELECT * FROM livestreams WHERE id = ?", livestreamID)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to get livestream: "+err.Error())
	}
	var tagsId []int64
	if err := tx.SelectContext(ctx, &tagsId, "SELECT `tag_id` FROM livestream_tags WHERE livestream_id = ?", livestreamModel.ID); err != nil {
		return fmt.Errorf("failed to get tags id: %w", err)
	}
	livestreamUser, err := getUserWithCache(ctx, livestreamModel.UserID)
	if err != nil {
		return fmt.Errorf("invalid user: %w", err)
	}

	reactions := make([]Reaction, len(reactionModels))
	for i := range reactionModels {
		reaction, err := fillReactionResponse(ctx, reactionModels[i], reactionUsers[reactionModels[i].UserID], &livestreamModel, tagsId, livestreamUser)
		if err != nil {
			return echo.NewHTTPError(http.StatusInternalServerError, "failed to fill reaction: "+err.Error())
		}

		reactions[i] = reaction
	}

	if err := tx.Commit(); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to commit: "+err.Error())
	}

	return c.JSON(http.StatusOK, reactions)
}

func postReactionHandler(c echo.Context) error {
	ctx := c.Request().Context()
	livestreamID, err := strconv.Atoi(c.Param("livestream_id"))
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "livestream_id in path must be integer")
	}

	if err := verifyUserSession(c); err != nil {
		// echo.NewHTTPErrorが返っているのでそのまま出力
		return err
	}

	// error already checked
	sess, _ := session.Get(defaultSessionIDKey, c)
	// existence already checked
	userID := sess.Values[defaultUserIDKey].(int64)

	var req *PostReactionRequest
	if err := json.NewDecoder(c.Request().Body).Decode(&req); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "failed to decode the request body as json")
	}

	tx, err := dbConn.BeginTxx(ctx, nil)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to begin transaction: "+err.Error())
	}
	defer tx.Rollback()

	reactionModel := ReactionModel{
		UserID:       int64(userID),
		LivestreamID: int64(livestreamID),
		EmojiName:    req.EmojiName,
		CreatedAt:    time.Now().Unix(),
	}

	livestreamModel := LivestreamModel{}
	err = tx.GetContext(ctx, &livestreamModel, "SELECT * FROM livestreams WHERE id = ?", livestreamID)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to get livestream: "+err.Error())
	}
	result, err := tx.NamedExecContext(ctx, "INSERT INTO reactions (user_id, livestream_id, emoji_name, created_at) VALUES (:user_id, :livestream_id, :emoji_name, :created_at)", reactionModel)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to insert reaction: "+err.Error())
	}

	if _, err := tx.ExecContext(ctx, "UPDATE livestreams SET reactions = reactions + 1 WHERE id = ?", livestreamID); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to update livestream reaction counter: "+err.Error())
	}

	if _, err := tx.ExecContext(ctx, "UPDATE users SET reactions = reactions + 1 WHERE id = ?", livestreamModel.UserID); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to update reactions: "+err.Error())
	}

	if _, err := tx.ExecContext(ctx, "INSERT INTO favorite_emojis (user_id, emoji_name) VALUES (?, ?)", livestreamModel.UserID, req.EmojiName); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to add favorite_emojis: "+err.Error())
	}

	reactionID, err := result.LastInsertId()
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to get last inserted reaction id: "+err.Error())
	}
	reactionModel.ID = reactionID

	reactionUser, err := getUserWithCache(ctx, userID)
	if err != nil {
		return fmt.Errorf("invalid user: %w", err)
	}
	var tagsId []int64
	if err := tx.SelectContext(ctx, &tagsId, "SELECT `tag_id` FROM livestream_tags WHERE livestream_id = ?", livestreamModel.ID); err != nil {
		return fmt.Errorf("failed to get tags id: %w", err)
	}
	livestreamUser, err := getUserWithCache(ctx, livestreamModel.UserID)
	if err != nil {
		return fmt.Errorf("invalid user: %w", err)
	}
	reaction, err := fillReactionResponse(ctx, reactionModel, reactionUser, &livestreamModel, tagsId, livestreamUser)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to fill reaction: "+err.Error())
	}

	if err := tx.Commit(); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to commit: "+err.Error())
	}

	return c.JSON(http.StatusCreated, reaction)
}

func fillReactionResponse(ctx context.Context, reactionModel ReactionModel, reactionUserModel *UserModel, livestreamModel *LivestreamModel, tagIds []int64, liveOwnerModel *UserModel) (Reaction, error) {
	user, err := fillUserResponse(ctx, reactionUserModel)
	if err != nil {
		return Reaction{}, err
	}
	livestream, err := fillLivestreamResponse(ctx, livestreamModel, liveOwnerModel, tagIds)
	if err != nil {
		return Reaction{}, err
	}

	reaction := Reaction{
		ID:         reactionModel.ID,
		EmojiName:  reactionModel.EmojiName,
		User:       user,
		Livestream: livestream,
		CreatedAt:  reactionModel.CreatedAt,
	}

	return reaction, nil
}
