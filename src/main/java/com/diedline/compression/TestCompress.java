package com.diedline.compression;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.FileInputStream;
import java.io.FileOutputStream;

public class TestCompress {
    public static void main(String[] args) throws Exception {
//        testCompress("f:/1.txt", "org.apache.hadoop.io.compress.BZip2Codec");
        testDeCompress("f:/1.txt.deflate");
    }

    /**
     * 解压缩
     *
     * @param name 文件路径
     */
    private static void testDeCompress(String name) throws Exception {
        //1.获取输入流
        FileInputStream fis = new FileInputStream(name);
        //2.获取编码器
        Configuration configuration = new Configuration();
        CompressionCodecFactory codecFactory = new CompressionCodecFactory(configuration);
        CompressionCodec codec = codecFactory.getCodec(new Path(name));
        if (codec == null) {
            System.out.println("can not find codec for this file" + name);
            return;
        }
        //3.获取压缩流
        CompressionInputStream cis = codec.createInputStream(fis);
        //4.获取输出流
        FileOutputStream fos = new FileOutputStream(name + ".decode");
        //5.流的拷贝
        IOUtils.copyBytes(cis, fos, configuration);
        //6.关闭资源
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
        IOUtils.closeStream(cis);
    }


    /**
     * 压缩
     *
     * @param path   文件路径
     * @param method 压缩方法
     */
    private static void testCompress(String path, String method) throws Exception {
        //1.获取输入流
        FileInputStream fileInputStream = new FileInputStream(path);
        //2.获取编解码器
        Class aClass = Class.forName(method);
        Configuration configuration = new Configuration();
        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(aClass, configuration);
        //3.获取文件输出流
        FileOutputStream fileOutputStream = new FileOutputStream(path + codec.getDefaultExtension());
        //4.获取文件压缩流
        CompressionOutputStream outputStream = codec.createOutputStream(fileOutputStream);
        //5.流的拷贝
        IOUtils.copyBytes(fileInputStream, fileOutputStream, configuration);
        //6.关闭资源
        IOUtils.closeStream(fileInputStream);
        IOUtils.closeStream(fileOutputStream);
        IOUtils.closeStream(outputStream);

    }
}
