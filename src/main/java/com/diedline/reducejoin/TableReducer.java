package com.diedline.reducejoin;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import javax.xml.soap.Text;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

public class TableReducer extends Reducer<Text, TableBean, NullWritable, TableBean> {


    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Context context) throws IOException, InterruptedException {
        //接收order表数据
        List<TableBean> orderList = new ArrayList<TableBean>();
        //接收pd表数据
        TableBean pdBean = new TableBean();

        for (TableBean tableBean : values) {
            //order表
            TableBean tbBean = new TableBean();
            if (tableBean.getFlag().equals("0")) {
                try {
                    //因为这里直接使用tableBean是使用的同一个地址即最后一个值 所以需要先拷贝添加临时变量
                    BeanUtils.copyProperties(tbBean, tableBean);
                    orderList.add(tbBean);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
            } else {
                //pd表
                try {
                    BeanUtils.copyProperties(tbBean, pdBean);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
            }

        }
        //循环更改名称然后将值赋给后面处理
        for (TableBean orderBean : orderList) {
            orderBean.setpName(pdBean.getpName());
            context.write(NullWritable.get(), orderBean);
        }
    }
}
