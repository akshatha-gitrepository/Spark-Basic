package utils;

import lombok.Data;

@Data
public class GameSession {
    private String playerId;
    private Long levelId;
    private int moves = 0;
    private int boosters = 0;
    private int score = 0;
    private Integer completedScore; // score from LEVEL_COMPLETE event
    private boolean completed = false;

    public double getEfficiency() {
        int effectiveScore = completed && completedScore != null ? completedScore : score;
        return moves > 0 ? ((double) effectiveScore) / moves : 0.0;
    }

    public void incrementMoves() {
        this.moves++;
    }

    public void incrementBoosters() {
        this.boosters++;
    }

    public void addScore(int value) {
        this.score += value;
    }

    public void setCompletedScore(int value) {
        this.completedScore = value;
    }

    public int getEffectiveScore() {
        return completed && completedScore != null ? completedScore : score;
    }

}
