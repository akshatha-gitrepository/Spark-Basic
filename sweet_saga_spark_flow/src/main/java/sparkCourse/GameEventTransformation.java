package sparkCourse;

import com.daimler.ca.spark.flow.application.IOperationContext;
import com.daimler.ca.spark.flow.operations.OperationException;
import com.daimler.ca.spark.flow.operations.Transformation1;
import com.daimler.ca.spark.flow.operations.WireInfo;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import schema.JsonSchema;
import utils.GameEvent;
import utils.GameEventFactory;

public class GameEventTransformation  extends Transformation1<Dataset<JsonSchema>, Dataset<GameEvent>> {
    public GameEventTransformation(IOperationContext context, WireInfo<?> i1, WireInfo<?> out) {
        super(context, i1, out);
    }

    /**
     * @param dataset
     * @return
     * @throws OperationException
     */
    @Override
    protected Dataset<GameEvent> work(Dataset<JsonSchema> dataset) throws OperationException {
        return dataset.toDF().map(
                (MapFunction<Row, GameEvent>) GameEventFactory::fromRow,
                Encoders.javaSerialization(GameEvent.class)
        );
    }
}
