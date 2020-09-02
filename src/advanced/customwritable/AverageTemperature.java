package advanced.customwritable;

import basic.WordCount;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class AverageTemperature {

    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path(files[0]);

        // arquivo de saida
        Path output = new Path(files[1]);

        // criacao do job e seu nome
        Job j = new Job(c, "forestfire-estudante");


        //Registrar classes
        j.setJarByClass(AverageTemperature.class);  //CLASSE PRINCIPAL
        j.setMapperClass(MapForAverage.class); //REGISTRAR DA CLASSE MAP
        j.setReducerClass(ReduceForAverage.class); //REGISTRO DA CLASSE REDUCE

        j.setCombinerClass(CombineTemperature.class);

        //Definir os tipos de saída (MAP E REDUCE)
        j.setMapOutputKeyClass(Text.class); //SAIDA CHAVE MAP
        j.setMapOutputValueClass(FireAvgTempWritable.class); // SAIDA VALOR MAP
        j.setOutputKeyClass(Text.class); // SAIDA REDUCE CHAVE
        j.setOutputValueClass(FloatWritable.class); // SAIDA REDUCE VALOR

        //Cadastrar os arquivos de entrada e saída
        FileInputFormat.addInputPath(j,input);
        FileOutputFormat.setOutputPath(j,output);

        // lanca o job e aguarda sua execucao
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }


    /**
     * 1ª Parametro: tipo da chave de entrada
     * 2º Parametro: tipo do valor de entrada
     * -----------------------------------------
     * 3º Parametro: tipo da chave de saída
     * 4º Parametro: tipo do valor de saída
     */
    public static class MapForAverage extends Mapper<LongWritable, Text, Text, FireAvgTempWritable> {

        // Funcao de map
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            ////Funcao de map eh executada por bloco, e na realidade eh executada por LINHA

            ///Obter o conteudo da linha

            String linha = value.toString();

            ///A partir da linha, obter apenas a coluna 8

            String [] colunas = linha.split(",");
            Float temperatura = Float.parseFloat(colunas[8]);
            Text mes = new Text(colunas[2]);
            IntWritable valorSaida = new IntWritable(1);
            ///Emitir<chave,valor>, sendo que a chave deve ser comum

            con.write(mes ,new FireAvgTempWritable(1,temperatura));

            //COMENTARIO IMPORTANTE!!
            //Usando sempre a mesmoa chave para garantir que todos os resultados originados de maps diferentes cheguem no mesmo reduce

            // e o valor deve ser composto (scma = valor, n=1)
        }
    }

    public static class ReduceForAverage extends Reducer<Text, FireAvgTempWritable, Text, FloatWritable> {

        // Funcao de reduce
        public void reduce(Text word, Iterable<FireAvgTempWritable> values, Context con)
                throws IOException, InterruptedException {

            int somaN = 0;
            float somaSomas = 0.0f;

            for( FireAvgTempWritable obj: values ){
                somaN+= obj.getN();
                somaSomas += obj.getSoma();
            }

            //calculando a media
            float media = somaSomas / somaN;

            //emitir o resultado final (media = X)
            con.write(word,new FloatWritable(media));

        }
    }

    public static class CombineTemperature extends  Reducer<Text, FireAvgTempWritable, Text, FireAvgTempWritable> {

        public void reduce(Text word, Iterable<FireAvgTempWritable> values, Context con) throws IOException, InterruptedException {
            float somaQuantidade = 0.0f;
            int quantidadeSoma = 0;

            for(FireAvgTempWritable vlr: values){
                quantidadeSoma++;
                    somaQuantidade += vlr.getSoma();
            }

            con.write(word,new FireAvgTempWritable(quantidadeSoma,somaQuantidade));
        }

    }

}
