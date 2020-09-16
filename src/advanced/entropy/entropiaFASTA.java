package advanced.entropy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class entropiaFASTA {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //Definir codigo padrao de rotinas map reduce

        // Definir codigo padrao de rotinas mapreduce
        BasicConfigurator.configure();

        Configuration c = new Configuration();

        // arquivo de entrada
        Path input = new Path("in/JY157487.1.fasta");

        // arquivo temporario
        Path intermediate = new Path("intermediario.txt");

        // arquivo de saida
        Path output = new Path("output/entropia.txt");

        // primeiro Job
        Job j1 = new Job(c, "primeira-parte");


        // registro das classes (JOB 1)
        j1.setJarByClass(entropiaFASTA.class);
        j1.setMapperClass(MapEapA.class);
        j1.setReducerClass(ReduceEtapaA.class);

        // definicao dos tipos de saida (JOB 1)
        // tipos de saida do map
        j1.setMapOutputKeyClass(Text.class);
        j1.setMapOutputValueClass(LongWritable.class);
        // tipos de saida do reduce
        j1.setOutputKeyClass(Text.class);
        j1.setOutputValueClass(LongWritable.class);

        // cadastro dos arquivos de entrada e saida (JOB 1)
        FileInputFormat.addInputPath(j1, input);
        FileOutputFormat.setOutputPath(j1, intermediate);

        // Lancar o job 1
        j1.waitForCompletion(true);

        // segundo job
        Job j2 = new Job(c, "segunda-parte");

        // registro de classes (JOB 2)
        j2.setJarByClass(entropiaFASTA.class);
        j2.setMapperClass(MapEtapaB.class);
        j2.setReducerClass(ReduceEtapaB.class);

        // definicao dos tipos de saida (JOB 2)
        j2.setMapOutputKeyClass(Text.class);
        j2.setMapOutputValueClass(Auxiliar.class);
        j2.setOutputKeyClass(Text.class);
        j2.setOutputValueClass(DoubleWritable.class);

        // cadastro dos arquivos de entrada e saida (JOB 2)
        FileInputFormat.addInputPath(j2, intermediate);
        FileOutputFormat.setOutputPath(j2, output);

        // lancar o job 2
        j2.waitForCompletion(true);


    }


    /**
     * 1 P - Tipo de
     */

    public static  class  MapEapA extends Mapper<LongWritable, Text,Text,LongWritable >{
        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {

            //executada por linha do arquivo de entrada
            String linha = value.toString();

            //Ignorando o cabeçalho: ignorando a linha se ela comeca com o sinal de >
            if(linha.startsWith(">")) return;

            //se não for o caso, vamos quebrar em caracteres e gerar (c,1)
            String[] bases = linha.split("");

            for (String b : bases){
                con.write(new Text(b),new LongWritable(1));
                con.write(new Text("total"),new LongWritable(1));
            }


        }
    }


    public static class  ReduceEtapaA extends Reducer<Text,LongWritable,Text,LongWritable>{
        public void reduce (Text key, Iterable<LongWritable> values, Context con) throws IOException, InterruptedException {
            // somando as ocorrencias de cada base (caracter)
            long soma = 0;
            for(LongWritable value: values){
                soma+= value.get();
            }

            //Como resultado final, vamos salvar em arquivo(base, qtd total)
            con.write(key,new LongWritable(soma));
        }
    }


    public static class MapEtapaB extends  Mapper<LongWritable, Text,Text,Auxiliar >{
        public void map(LongWritable key, Text value,Context con) throws IOException, InterruptedException {
            String[] colunas = value.toString().split("\t");

            //colunas[0] => base, colunas[1] => quantidade
            con.write(new Text("agg"),new Auxiliar(colunas[0],Long.parseLong(colunas[1])));
            // -> 'agg' : ('A' : 125)
        }
    }

    public static class  ReduceEtapaB extends Reducer<Text,Auxiliar,Text,DoubleWritable>{
        public void reduce(Text key,Iterable<Auxiliar> values, Context con) throws IOException, InterruptedException {
            long qtdTotal= 0;

            //1o objetivo: encontrar o total e a sua quantidade
            for(Auxiliar a: values){
                if(a.getBase().equals("total")){
                    qtdTotal = a.getQtd(); //encontrei!!
                }
                break;
            }

            //2o objetivo: calcular a probabilidade e consequentemente a entropia
            for(Auxiliar a: values){
                if(!a.getBase().equals("total")){
                    //calcular a probabilidade
                    double p = a.getQtd() / (double) qtdTotal;
                    //calcular entropia log_2 (x) = log_10(x) / log10(2)
                    double entropia = -p* Math.log10(p)/Math.log10(2.0);
                    //salvar o resultado
                    con.write(new Text(a.getBase()),new DoubleWritable(entropia));
                }
            }
        }


    }


}
