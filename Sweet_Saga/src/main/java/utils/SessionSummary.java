package utils;

import lombok.Builder;
import lombok.Data;

import java.io.Serializable;

@Data
@Builder
public class SessionSummary implements Serializable {
    private String playerId;
    private Long levelId;
    private Integer moves;
    private Integer boosters;
    private Integer score;
    private Boolean completed;
    private Double efficiency;
}
