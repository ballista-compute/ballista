import io.andygrove.ballista.client.Client
import io.andygrove.kquery.datasource.CsvDataSource
import io.andygrove.kquery.logical.*
import org.junit.Ignore
import org.junit.Test
import org.junit.jupiter.api.TestInstance

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class DataFrameTest {

    @Test
    fun test() {

        val df = DataFrameImpl(Scan("", CsvDataSource("", 0), listOf()))

        val df2 = df
            .filter(col("a") eq lit(123))
            .select(listOf(col("a"), col("b"), col("c")))

        val client = Client("localhost", 50001)
        client.execute(df2.logicalPlan())
    }
}