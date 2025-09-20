package events;

import utils.GameEvent;
import utils.GameSession;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

@Data
@AllArgsConstructor
public class SwapEvent extends GameEvent implements Serializable {

    private String playerId;
    private String colorA;
    private String colorB;
    private Long levelId;

    @Override
    public void apply(GameSession session) {
        session.incrementMoves();
        session.addScore(10);
    }
}
