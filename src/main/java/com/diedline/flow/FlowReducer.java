package com.diedline.flow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
    private FlowBean flowBean = new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        //1.累加
        long sumUpFlow = 0;
        long sumDownFlow = 0;
        for (FlowBean flow : values
        ) {
            sumUpFlow += flow.getUpflow();
            sumDownFlow += flow.getDownflow();
        }

        //封装对象
        flowBean.set(sumUpFlow, sumDownFlow);
        //写数据
        context.write(key, flowBean);
    }
}
