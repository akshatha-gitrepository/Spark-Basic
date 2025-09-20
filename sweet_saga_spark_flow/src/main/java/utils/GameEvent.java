package utils;

import java.io.Serializable;

public abstract class GameEvent implements Serializable {

    public abstract String getPlayerId();
    public abstract Long getLevelId();
    public abstract void apply(GameSession session);
}

