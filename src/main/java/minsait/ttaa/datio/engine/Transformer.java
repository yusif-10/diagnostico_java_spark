package minsait.ttaa.datio.engine;

import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.jetbrains.annotations.NotNull;

import static minsait.ttaa.datio.common.Common.*;
import static minsait.ttaa.datio.common.naming.PlayerInput.*;
import static minsait.ttaa.datio.common.naming.PlayerInput.nationality;
import static minsait.ttaa.datio.common.naming.PlayerOutput.*;
import static org.apache.spark.sql.functions.*;
import static minsait.ttaa.datio.common.Variables.*;

public class Transformer extends Writer {
    private SparkSession spark;

    public Transformer(@NotNull SparkSession spark) {
        this.spark = spark;
        Dataset<Row> df = readInput();

        df.printSchema();

        df = cleanData(df);
        df = exampleWindowFunction(df);
        df = AgeRange(df);
        df = rangeByNationality(df);
        df = potentialVsOverall(df);
        df = filter(df);
        df = columnSelection(df);

        // for show 100 records after your transformations and show the Dataset schema
        df.show(cien, false);
        df.printSchema();

        // Uncomment when you want write your final output
        write(df);
    }

    private Dataset<Row> columnSelection(Dataset<Row> df) {
        return df.select(
                shortName.column(),
                longName.column(),
                age.column(),
                heightCm.column(),
                weightKg.column(),
                nationality.column(),
                clubName.column(),
                overall.column(),
                potential.column(),
                teamPosition.column(),
                ageRange.column(),
                catHeightByPosition.column(),
                rangeByNationality.column(),
                potentialVsOverall.column()
        );
    }

    /**
     * @return a Dataset readed from csv file
     */
    private Dataset<Row> readInput() {
        Dataset<Row> df = spark.read()
                .option(HEADER, true)
                .option(INFER_SCHEMA, true)
                .csv(INPUT_PATH);
        return df;
    }

    /**
     * @param df
     * @return a Dataset with filter transformation applied
     * column team_position != null && column short_name != null && column overall != null
     */
    private Dataset<Row> cleanData(Dataset<Row> df) {
        df = df.filter(
                teamPosition.column().isNotNull().and(
                        shortName.column().isNotNull()
                ).and(
                        overall.column().isNotNull()
                )
        );

        return df;
    }

    /**
     * @param df is a Dataset with players information (must have team_position and height_cm columns)
     * @return add to the Dataset the column "cat_height_by_position"
     * by each position value
     * cat A for if is in 20 players tallest
     * cat B for if is in 50 players tallest
     * cat C for the rest
     */
    private Dataset<Row> exampleWindowFunction(Dataset<Row> df) {
        WindowSpec w = Window
                .partitionBy(teamPosition.column())
                .orderBy(heightCm.column().desc());

        Column rank = rank().over(w);

        Column rule = when(rank.$less(diez), A)
                .when(rank.$less(cincuenta), B)
                .otherwise(C);

        df = df.withColumn(catHeightByPosition.getName(), rule);

        return df;
    }

    /**
     * @return add to the Dataset the column "age_range"
     * by each position value
    A si el jugador es menor de 23 años
    B si el jugador es menor de 27 años
    C si el jugador es menor de 32 años
    D si el jugador tiene 32 años o más
     */
    private Dataset<Row> AgeRange(Dataset<Row> df) {
        df = df.withColumn(ageRange.getName(),
                when(df.col(columnAge).$less(veintitres),A)
                        .otherwise(when(df.col(columnAge).$less(veintisiete).and(df.col(columnAge).$greater$eq(veintitres)),B)
                        .otherwise(when(df.col(columnAge).$less(treintaYdos).and(df.col(columnAge).$greater$eq(veintisiete)),C)
                                .otherwise(when(df.col(columnAge).$greater$eq(treintaYdos),D)))
                        ));
        return df;
    }


    /**
     * Agregaremos una columna rank_by_nationality_position con la siguiente regla: Para cada país (nationality)
     * y posición(team_position) debemos ordenar a los jugadores por la columna overall de forma descendente
     * y colocarles un número generado por la función row_number
     */
    private Dataset<Row> rangeByNationality(Dataset<Row> df) {
        WindowSpec w = Window
                .partitionBy(nationality.column(),teamPosition.column())
                .orderBy(overall.column().desc());
        df=df.withColumn(rangeByNationality.getName(),row_number().over(w));
        return df;
    }

    /**
     * Agregaremos una columna potential_vs_overall cuyo valor estará definido por la siguiente regla:
    potential_vs_overall = potential/overall
     */
    private Dataset<Row> potentialVsOverall(Dataset<Row> df) {
        df = df.withColumn(potentialVsOverall.getName(),df.col(columnPotential).divide(col(columnOverall)));
        return df;
    }

    /**
     * Filtraremos de acuerdo a las columnas age_range y rank_by_nationality_position con las siguientes condiciones:
     Si rank_by_nationality_position es menor a 3
     Si age_range es B o C y potential_vs_overall es superior a 1.15
     Si age_range es A y potential_vs_overall es superior a 1.25
     Si age_range es D y rank_by_nationality_position es menor a 5

     Tuve duda en los filtros ya que si aplicaba "and" no me mostraba ningun registro
     df = df.filter(col(columnRankByNationalityPosition).$less(tres)
     .and(col(columnAgeRange).equalTo(B).or(col(columnAgeRange).equalTo(C)).and(col(columnPontentialVsOverall).$greater(unoPuntoQuince)))
     .and(col(columnAgeRange).equalTo(A).and(col(columnPontentialVsOverall).$greater(unoPuntoVenticinco)))
     .and(col(columnAgeRange).equalTo(D).and(col(columnRankByNationalityPosition).$less(cinco)))
     );
     */
    private Dataset<Row> filter(Dataset<Row> df) {
        df = df.filter(col(columnRankByNationalityPosition).$less(tres)
                .or(col(columnAgeRange).equalTo(B).or(col(columnAgeRange).equalTo(C)).and(col(columnPontentialVsOverall).$greater(unoPuntoQuince)))
              .or(col(columnAgeRange).equalTo(A).and(col(columnPontentialVsOverall).$greater(unoPuntoVenticinco)))
            .or(col(columnAgeRange).equalTo(D).and(col(columnRankByNationalityPosition).$less(cinco)))
                    );
        return df;
    }


}
