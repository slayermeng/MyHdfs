package org.myhdfs.compress;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;
import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.net.URI;

/**
 * Created with IntelliJ IDEA.
 * User: mengxin
 * Date: 13-10-10
 * Time: 下午4:21
 * To change this template use File | Settings | File Templates.
 */
public class Compress {
    public static void compress(String path,String algorithm) throws FileNotFoundException,ClassNotFoundException,IOException{
        File fileIn = new File(path);
        InputStream in = new FileInputStream(fileIn);
        Class<?> codecClass = Class.forName(algorithm);
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        CompressionCodec codec = (CompressionCodec)ReflectionUtils.newInstance(codecClass,conf);
        FSDataOutputStream fileOut = fs.create(new Path(fileIn.getName()+codec.getDefaultExtension()));
        CompressionOutputStream cout = codec.createOutputStream(fileOut);
        IOUtils.copyBytes(in, cout, 4096, false);
        IOUtils.closeStream(in);
        IOUtils.closeStream(fileOut);
    }

    public static void uncompress(String uri) throws IOException{
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri), conf);
        Path inputPath = new Path(uri);
        CompressionCodecFactory factory = new CompressionCodecFactory(conf);
        CompressionCodec codec = factory.getCodec(inputPath);
        if(codec == null){
        System.out.println("no codec found for " + uri);
        System.exit(1);
        }
        String outputUri = CompressionCodecFactory.removeSuffix(uri, codec.getDefaultExtension());
        InputStream in = null;
        OutputStream out = null;
        try {
            in = codec.createInputStream(fs.open(inputPath));
            out = fs.create(new Path(outputUri));
            IOUtils.copyBytes(in, out, conf);
        } finally{
            IOUtils.closeStream(out);
            IOUtils.closeStream(in);
        }
    }

    public static void main(String[] args) throws Exception{
        if(args[0].equals("compress")){
            compress(args[1],args[2]);
        }else if(args[0].equals("uncompress")){
            uncompress(args[1]);
        }
    }
}
