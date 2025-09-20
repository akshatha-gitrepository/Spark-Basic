package SparkCourse;

import com.daimler.ca.properties.Configures;
import com.daimler.ca.properties.DocuRoot;
import com.daimler.ca.properties.PropertyReader;
import com.daimler.ca.spark.flow.application.FlowApplication;
import com.daimler.ca.spark.flow.application.IOperationContext;
import com.daimler.ca.spark.flow.operations.OperationPool;
import org.apache.spark.sql.SparkSession;

@DocuRoot(name = "Candy crush", namespace = "spark")
@Configures(value = CandyCrushPipeline.class, namespace = "candy")
public class CandyCrushPipeline extends FlowApplication {

    public static void main(String[] args) {

        SparkSession session = getOrCreateFromCommandLineConfig(args);
        // Create application instance
        CandyCrushPipeline s = new CandyCrushPipeline(session);
        // Run the application. We'll look into the lifecycle later.
        if (!s.run()) {
            throw new IllegalStateException("Pipeline failed.");
        }
    }

    private CandyCrushPipeline(SparkSession session) {
        super(session);
    }

    /**
     * @param ctx
     * @param propertyReader
     * @return
     */
    @Override
    protected OperationPool initializePool(IOperationContext ctx, PropertyReader propertyReader) {

        // Configure and return the pool.
        return reader.configure( new CandyCrushOperationPool(ctx),"candy");
    }

}