package events;

import lombok.AllArgsConstructor;
import lombok.Data;
import utils.GameEvent;
import utils.GameSession;


import java.io.Serializable;

// LifeLostEvent.java
@Data
@AllArgsConstructor
public  class LifeLostEvent extends GameEvent implements Serializable {

    private String playerId;
    private Long levelId;

    @Override
    public void apply(GameSession session) {
        session.addScore(-200);
    }

}