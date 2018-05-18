/**
 * 在做运营统计的时候，一个最常见的指标是日活跃用户数(DAU)，它的一般性概念为当日所有用户的去重，但是在大部分情况下，我们获取到的数据中会有登录用户与有匿名
 * 用户，而这部分用户是会出现重叠的。常规的做法是利用cookie或者imei(移动端)进行自关联，然后算出有多少用户同时是登录用户和匿名用户，最终的 日活跃用户数 
 * = 登录用户+匿名用户-匿名转登录用户。
 * 根据flag,uid和imei信息计算个数
 * -flag为1	  ： 将对应的UID存储在UID集合中，该集合代表登录用户
 * -flag不为1 ： 将对应的imei|wyy存储在IMEI集合中，该集合代表匿名用户
 * 将imei|wyy存储一个Map当中，并且判断该imei|wyy对应的flag是否同时出现过0和1俩个值，如果是则map中对应的value = 2否则为flag
 * 参数原型:
 *      int itemcount(flag,uid,imei)
 * 参数说明:
 *      flag: 1或者不为1
 *      uid	: 用户id
 *      imei: 用户的第二个参照标识(imei|wyy|cookie)
 *      
 * 返回值:
 *      int类型，dau值
 *      
 * 使用示例:
 *      > SELECT flag, uid, imei FROM test;
 *      1   uid1 imei1
 *      1   uid2 imei1
 *      0   uid3 imei3
 * 
 *      > SELECT daucount(flag,uid,imei) FROM test;
 *      1
 */
package com.matthewrathbone.example;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardMapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;


@Description (
        name = "dau_count",
        value = "_FUNC_(flag,uid,imei)"
        )
public class GenericDauCount extends AbstractGenericUDAFResolver {

    private static final boolean DEBUG = false;
    private static final boolean TRACE = false;

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
            throws SemanticException {

        if (parameters.length != 3) {
            throw new UDFArgumentLengthException(
                    "Exactly 3 argument is expected.");
        }

        if (((PrimitiveTypeInfo) parameters[0]).getPrimitiveCategory() != PrimitiveCategory.INT) {
            throw new UDFArgumentTypeException(0,
                    "Only int argument is accepted, but "
                            + parameters[0].getTypeName() + " is passed");
        }

        if (((PrimitiveTypeInfo) parameters[1]).getPrimitiveCategory() != PrimitiveCategory.STRING) {
            throw new UDFArgumentTypeException(1,
                    "Only string argument is accepted, but "
                            + parameters[1].getTypeName() + " is passed");
        }

        if (((PrimitiveTypeInfo) parameters[2]).getPrimitiveCategory() != PrimitiveCategory.STRING) {
            throw new UDFArgumentTypeException(2,
                    "Only string argument is accepted, but "
                            + parameters[2].getTypeName() + " is passed");
        }

        return new GenericDauCountEvaluator();
    }

    public static class GenericDauCountEvaluator extends GenericUDAFEvaluator {
        // 封装接口
        StructField uidSetField;
        StructField imeiSetField;
        StructField imeiMapField;

        StructObjectInspector map2red;

        // for PARTIAL1 and COMPLETE
        // map阶段的时候的输入流,就是三个列的输入
        IntObjectInspector flagIO;
        StringObjectInspector uidIO;
        StringObjectInspector imeiIO;

        // for PARTIAL2 and FINAL
        // 在map和reduce阶段的输入，是uidSet(登陆用户的id)，imeiSet(匿名用户的id)，imeiMap()
        StandardListObjectInspector uidSetIO;
        StandardListObjectInspector imeiSetIO;
        StandardMapObjectInspector imeiMapIO;

        private static class DivideAB implements AggregationBuffer {
            Set<String> uidSet;
            Set<String> imeiSet;
            Map<String, Integer> imeiMap;
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            DivideAB dab = new DivideAB();
            reset(dab);
            return dab;
        }

        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            DivideAB dab = (DivideAB) agg;
            dab.uidSet = new HashSet<String>();
            dab.imeiSet = new HashSet<String>();
            dab.imeiMap = new HashMap<String, Integer>();
        }

        boolean warned = false;

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters)
                throws HiveException {
            super.init(m, parameters);

            // input
            if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) { // for iterate
                assert (parameters.length == 3);
                flagIO = (IntObjectInspector) parameters[0];
                uidIO = (StringObjectInspector) parameters[1];
                imeiIO = (StringObjectInspector) parameters[2];
            } else { // for merge,map
                map2red = (StructObjectInspector) parameters[0];
                uidSetField = map2red.getStructFieldRef("uidSet");
                imeiSetField = map2red.getStructFieldRef("imeiSet");
                imeiMapField = map2red.getStructFieldRef("imeiMap");

                uidSetIO = (StandardListObjectInspector) uidSetField
                        .getFieldObjectInspector();
                imeiSetIO = (StandardListObjectInspector) imeiSetField
                        .getFieldObjectInspector();
                imeiMapIO = (StandardMapObjectInspector) imeiMapField
                        .getFieldObjectInspector();
            }
            // output
            if (m == Mode.PARTIAL1 || m == Mode.PARTIAL2) {
                ArrayList<ObjectInspector> foi = new ArrayList<ObjectInspector>();
                ArrayList<String> fname = new ArrayList<String>();

                foi.add(ObjectInspectorFactory
                        .getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector));
                foi.add(ObjectInspectorFactory
                        .getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector));
                foi.add(ObjectInspectorFactory
                        .getStandardMapObjectInspector(
                                PrimitiveObjectInspectorFactory.javaStringObjectInspector,
                                PrimitiveObjectInspectorFactory.javaIntObjectInspector));
                fname.add("uidSet");
                fname.add("imeiSet");
                fname.add("imeiMap");
                //生成上面的map2red(StructObjectInspector)
                return ObjectInspectorFactory.getStandardStructObjectInspector(
                        fname, foi);
            } else {//reduce
                return PrimitiveObjectInspectorFactory.javaLongObjectInspector;
            }
        }

        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters)
                throws HiveException {
            if (parameters.length != 3) {
                return;
            }
            DivideAB dab = (DivideAB) agg;
            int check = PrimitiveObjectInspectorUtils.getInt(parameters[0],
                    flagIO);
            String uid = PrimitiveObjectInspectorUtils.getString(parameters[1],
                    uidIO);
            String imei = PrimitiveObjectInspectorUtils.getString(
                    parameters[2], imeiIO);
            if (check == 1) {// 登录用户
                dab.uidSet.add(uid);
            } else {// 匿名用户
                dab.imeiSet.add(imei);
            }
            if (dab.imeiMap.containsKey(imei)) {
                int flag = dab.imeiMap.get(imei);
                if (flag < 2 && flag != check) {
                    dab.imeiMap.put(imei, 2);
                }
            } else {
                dab.imeiMap.put(imei, check);
            }
        }

        @Override
        public Object terminatePartial(AggregationBuffer agg)
                throws HiveException {
            DivideAB myagg = (DivideAB) agg;
            // 存储中间结果,partialResult就是merge时使用的partial
            Object[] partialResult = new Object[3];
            partialResult[0] = new ArrayList<String>(myagg.uidSet);
            partialResult[1] = new ArrayList<String>(myagg.imeiSet);
            partialResult[2] = new HashMap<String, Integer>(myagg.imeiMap);
            return partialResult;
        }

        @SuppressWarnings("unchecked")
        @Override
        public void merge(AggregationBuffer agg, Object partial)
                throws HiveException {
            if (partial != null) {
                DivideAB dab = (DivideAB) agg;
                //得到map2red中不同的解析器，两个set和一个ma
                Object uidSet = map2red
                        .getStructFieldData(partial, uidSetField);
                Object imeiSet = map2red.getStructFieldData(partial,
                        imeiSetField);
                Object imeiMap = map2red.getStructFieldData(partial,
                        imeiMapField);

                List<Object> uidlist = (List<Object>) uidSetIO.getList(uidSet);
                System.err.println("uidList = " + uidlist.size());
                
                if (uidlist != null) {
                    System.err.println("uidSet = " + dab.uidSet.size());
                    for (Object obj : uidlist) {
                        dab.uidSet.add(obj.toString());
                    }
                }

                List<Object> imeilist = (List<Object>) uidSetIO
                        .getList(imeiSet);
                if (imeilist != null) {
                    for (Object obj : imeilist) {
                        dab.imeiSet.add(obj.toString());
                    }
                }

                Map<String, Integer> imeimap = (Map<String, Integer>) imeiMapIO
                        .getMap(imeiMap);
                for (Entry<?, ?> ele : imeimap.entrySet()) {
                    Object kobj = ele.getKey();
                    String key = kobj.toString();
                    Object vobj = ele.getValue();
                    Object val = vobj.toString();
                    if (dab.imeiMap.containsKey(key)) {
                        int flag = dab.imeiMap.get(key);
                        if (flag < 2
                                && flag != Integer.parseInt(val.toString())) {
                            dab.imeiMap.put(key, 2);
                        }
                    } else {
                        dab.imeiMap.put(key, Integer.parseInt(val.toString()));
                    }
                }
            }
        }

        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            DivideAB dab = (DivideAB) agg;
            int mix = 0;
            for (int val : dab.imeiMap.values()) {
                if (val == 2) {
                    mix++;
                }
            }
            return (long) (dab.uidSet.size() + dab.imeiSet.size() - mix);
        }
    }
}
