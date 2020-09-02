package basic;

import java.io.IOException;

import advanced.WordCountCombiner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;


public class WordCount {

    public static void main(String[] args) throws Exception {

        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path(files[0]);

        // arquivo de saida
        Path output = new Path(files[1]);

        // criacao do job e seu nome
        Job j = new Job(c, "wordcount-professor");

        //Registro das classes
        j.setJarByClass(WordCount.class);
        j.setMapperClass(MapForWordCount.class);
        j.setReducerClass(ReduceForWordCount.class);

        j.setCombinerClass(CombinerForWordCount.class);

        //Definicao dos tipos de saida(reduce)
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(IntWritable.class);

        //cadastro dos arquivos de entrada e saida
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        // lanca o job e aguarda sua execucao
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    public static class MapForWordCount extends Mapper<LongWritable, Text, Text, IntWritable> {

        // Funcao de map
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {


            // lembrete: o map é executado por bloco de arquivo
            //  contudo, nos vamos ver que na realidade, o map é executado por LINHA do arquivo

            String linha = value.toString();

            ///separando por espaços

            String[] palavras = linha.split(" ");

            // Loop -> gerar as tuplas no formato de <chave = palavra, valor = ocorrencia = 1>

            for (String p : palavras) {

                //criando chave de saida
                Text chaveSaida = new Text(p);

                //criando valor de saida
                IntWritable valorSaida = new IntWritable(1);

                //Emitir a tupla

                con.write(chaveSaida, valorSaida);

            }
        }
    }

    public static class ReduceForWordCount extends Reducer<Text, IntWritable, Text, IntWritable> {

        // Funcao de reduce
        public void reduce(Text word, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException {

            //Objetivo do reduce: fazer um loop para somar todas as ocorrencias de uma palavra
            // E no final, emitir a palavra com a ocorrencia ja somada.

            int soma = 0;

            for (IntWritable valor : values) {
                soma += valor.get();
            }


            //criando tupla com o resultado final(palavra,soma)
            con.write(word, new IntWritable(soma));
            //lembrete: no reduce, o write escreve no arquivo!

        }

    }

    /**
     * Um combiner é muito parecido com o reducer, ele é executado por CHAVE apos a execucao de cada MAP
     * O combiner é executado por BLOCO assim como o MAP
     */
    public static class CombinerForWordCount extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text word, Iterable<IntWritable> values, Context con) throws IOException, InterruptedException {
            int soma = 0;
            for (IntWritable vlr : values) {
                soma += vlr.get();
            }
            con.write(word, new IntWritable(soma));
        }


    }
}