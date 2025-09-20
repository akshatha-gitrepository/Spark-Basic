package schema;


import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class JsonSchema implements Serializable {

    @JsonProperty("playerId")
    private String playerId;

    @JsonProperty("timestamp")
    private String timestamp;

    @JsonProperty("levelId")
    private Long levelId;

    @JsonProperty("eventType")
    private String eventType;

    @JsonProperty("eventData")
    private EventData eventData;

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class EventData implements Serializable {

        @JsonProperty("score")
        private String score;

        @JsonProperty("type")
        private String type;

        @JsonProperty("colorA")
        private String colorA;

        @JsonProperty("colorB")
        private String colorB;
    }
}
