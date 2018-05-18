package com.matthewrathbone.example;

import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.StringUtils;
 
/**
 * GenericUDAFMaxMin.
 */
@Description(name = "maxmin", value = "_FUNC_(x) - Returns the max and min value of a set of numbers")
public class GenericUDAFMaxMin extends AbstractGenericUDAFResolver {
 
    static final Log LOG = LogFactory.getLog(GenericUDAFMaxMin.class.getName());
 
    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
            throws SemanticException {
        if (parameters.length != 1) {
            throw new UDFArgumentTypeException(parameters.length - 1,
                    "Exactly one argument is expected.");
        }
 
        if (parameters[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(0,
                    "Only primitive type arguments are accepted but "
                            + parameters[0].getTypeName() + " is passed.");
        }
        switch (((PrimitiveTypeInfo) parameters[0]).getPrimitiveCategory()) {
        case BYTE:
        case SHORT:
        case INT:
        case LONG:
        case FLOAT:
        case DOUBLE:
        case STRING:
        case TIMESTAMP:
            return new GenericUDAFMaxMinEvaluator();
        case BOOLEAN:
        default:
            throw new UDFArgumentTypeException(0,
                    "Only numeric or string type arguments are accepted but "
                            + parameters[0].getTypeName() + " is passed.");
        }
    }
 
    /**
     * GenericUDAFMaxMinEvaluator.
     *
     */
    public static class GenericUDAFMaxMinEvaluator extends GenericUDAFEvaluator {
 
        // For PARTIAL1 and COMPLETE
        PrimitiveObjectInspector inputOI;
 
        // For PARTIAL2 and FINAL
        StructObjectInspector soi;
        // 封装好的序列化数据接口，存储计算过程中的最大值与最小值
        StructField maxField;
        StructField minField;
        // 存储数据，利用get()可直接返回double类型值
        DoubleObjectInspector maxFieldOI;
        DoubleObjectInspector minFieldOI;
 
        // For PARTIAL1 and PARTIAL2
        // 存储中间的结果
        Object[] partialResult;
 
        // For FINAL and COMPLETE
        // 最终输出的数据
        Text result;
 
        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters)
                throws HiveException {
            assert (parameters.length == 1);
            super.init(m, parameters);
 
            // 初始化数据输入过程
            if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
                inputOI = (PrimitiveObjectInspector) parameters[0];
            } else {
                // 如果接收到的数据是中间数据，则转换成相应的结构体
                soi = (StructObjectInspector) parameters[0];
                // 获取指定字段的序列化数据
                maxField = soi.getStructFieldRef("max");
                minField = soi.getStructFieldRef("min");
                // 获取指定字段的实际数据
                maxFieldOI = (DoubleObjectInspector) maxField.getFieldObjectInspector();
                minFieldOI = (DoubleObjectInspector) minField.getFieldObjectInspector();
            }
 
            // 初始化数据输出过程
            if (m == Mode.PARTIAL1 || m == Mode.PARTIAL2) {
                // 输出的数据是一个结构体，其中包含了max和min的值
                // 存储结构化数据类型
                ArrayList<ObjectInspector> foi = new ArrayList<ObjectInspector>();
                foi.add(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
                foi.add(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
                // 存储结构化数据的字段名称
                ArrayList<String> fname = new ArrayList<String>();
                fname.add("max");
                fname.add("min");
                partialResult = new Object[2];
                partialResult[0] = new DoubleWritable(0);
                partialResult[1] = new DoubleWritable(0);
                return ObjectInspectorFactory.getStandardStructObjectInspector(fname,
                        foi);
 
            } else {
                // 如果执行到了最后一步，则指定相应的输出数据类型
                result = new Text("");
                return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
            }
        }
 
        static class AverageAgg implements AggregationBuffer {
            double max;
            double min;
        };
 
        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            AverageAgg result = new AverageAgg();
            reset(result);
            return result;
        }
 
        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            AverageAgg myagg = (AverageAgg) agg;
            myagg.max = Double.MIN_VALUE;
            myagg.min = Double.MAX_VALUE;
        }
 
        boolean warned = false;
 
        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters)
                throws HiveException {
            assert (parameters.length == 1);
            Object p = parameters[0];
            if (p != null) {
                AverageAgg myagg = (AverageAgg) agg;
                try {
                    // 获取输入数据，并进行相应的大小判断
                    double v = PrimitiveObjectInspectorUtils.getDouble(p, inputOI);
                    if(myagg.max < v){
                        myagg.max = v;
                    }
                    if(myagg.min > v){
                        myagg.min = v;
                    }
                } catch (NumberFormatException e) {
                    if (!warned) {
                        warned = true;
                        LOG.warn(getClass().getSimpleName() + " "
                                + StringUtils.stringifyException(e));
                        LOG.warn(getClass().getSimpleName()
                                + " ignoring similar exceptions.");
                    }
                }
            }
        }
 
        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            // 将中间计算出的结果封装好返回给下一步操作
            AverageAgg myagg = (AverageAgg) agg;
            ((DoubleWritable) partialResult[0]).set(myagg.max);
            ((DoubleWritable) partialResult[1]).set(myagg.min);
            return partialResult;
        }
 
        @Override
        public void merge(AggregationBuffer agg, Object partial)
                throws HiveException {
            if (partial != null) {
                //此处partial接收到的是terminatePartial的输出数据
                AverageAgg myagg = (AverageAgg) agg;
                Object partialmax = soi.getStructFieldData(partial, maxField);
                Object partialmin = soi.getStructFieldData(partial, minField);
                if(myagg.max < maxFieldOI.get(partialmax)){
                    myagg.max = maxFieldOI.get(partialmax);
                }
                if(myagg.min > minFieldOI.get(partialmin)){
                    myagg.min = minFieldOI.get(partialmin);
                }
            }
        }
 
        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            // 将最终的结果合并成字符串后输出
            AverageAgg myagg = (AverageAgg) agg;
            if (myagg.max == 0) {
                return null;
            } else {
                result.set(myagg.max + "\t" + myagg.min);
                return result;
            }
        }
    }
 
}
