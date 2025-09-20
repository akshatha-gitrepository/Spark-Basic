package sparkCourse;

import com.daimler.ca.properties.Configures;
import com.daimler.ca.properties.PropertyReader;
import com.daimler.ca.spark.flow.application.IOperationContext;
import com.daimler.ca.spark.flow.operations.*;
import com.daimler.ca.spark.flow.operations.batch.reader.FileReader;
import com.daimler.ca.spark.flow.operations.batch.writer.FileWriter;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import schema.JsonSchema;
import utils.GameEvent;
import utils.SessionSummary;

@Configures(value = FileReader.class, namespace = "input")
@Configures(value = FileWriter.class,namespace = "output")
public class CandyCrushOperationPool extends OperationPool {

    public CandyCrushOperationPool(IOperationContext ctx) {
        super(ctx);
    }

    @Override
    public void configure(PropertyReader reader) {
        super.configure(reader);

        // 1. Create FileReader for JsonSchema data
        FileReader<JsonSchema> inputData = new FileReader<>(this, JsonSchema.class);

        add(inputData,"input");

        // 2. Create wire info for connecting operations
        WireInfo<?> jsonSchemaWireInfo = inputData.getOutWireInfo(); // Input wire from FileReader
        WireInfo<?> rowWireInfo = new WireInfo<>(IO.DS, GameEvent.class);  // Output wire with Row data

        GameEventTransformation transformer = new GameEventTransformation(
                this,jsonSchemaWireInfo,new WireInfo<>(IO.DS, GameEvent.class));

        SessionSummaryTransformation processor = new SessionSummaryTransformation(
                this, rowWireInfo, new WireInfo<>(IO.DS,SessionSummary.class)
        );

//        // 3. Create the MapGroupAndSummarize processor
//        MapGroupAndSummarizeGameSession processor = new MapGroupAndSummarizeGameSession(
//                this, jsonSchemaWireInfo, new WireInfo<>(IO.DS, SessionSummary.class));

        // 5. Connect the operations
        inputData.connectTo(transformer);
        //inputData.connectTo(processor);
        transformer.connectTo(processor);


        // 6. Add it to the pool as an output operation
        FileWriter<SessionSummary> output = new FileWriter<>(this,SessionSummary.class);
        processor.connectTo(output);

        add(output,"output", true);
        output.setMode(SaveMode.Overwrite);

    }
}
