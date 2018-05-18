package com.matthewrathbone.example;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.util.StringUtils;

@Description(name = "median", value = "" + "_FUNC_(x) return the median number of a number array. eg: median(x)")
public class GenericUDAFMedian extends AbstractGenericUDAFResolver {

	static final Log LOG = LogFactory.getLog(GenericUDAFMedian.class.getName());

	@Override
	public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
		if (parameters.length != 1) {
			throw new UDFArgumentTypeException(parameters.length - 1, "Only 1 parameter is accepted!");
		}

		ObjectInspector objectInspector = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(parameters[0]);
		if (!ObjectInspectorUtils.compareSupported(objectInspector)) {
			throw new UDFArgumentTypeException(parameters.length - 1,
					"Cannot support comparison of map<> type or complex type containing map<>.");
		}

		switch (((PrimitiveTypeInfo) parameters[0]).getPrimitiveCategory()) {
		case BYTE:
		case SHORT:
		case INT:
			return new GenericUDAFMedianEvaluatorInt();
		case LONG:
			return new GenericUDAFMedianEvaluatorLong();
		case FLOAT:
		case DOUBLE:
			return new GenericUDAFMedianEvaluatorDouble();
		case STRING:
		case BOOLEAN:
		default:
			throw new UDFArgumentTypeException(0, "Only numeric type(int long double) arguments are accepted but "
					+ parameters[0].getTypeName() + " was passed as parameter of index->1.");
		}
	}

	public static class GenericUDAFMedianEvaluatorInt extends GenericUDAFEvaluator {

		private DoubleWritable result = new DoubleWritable();
		PrimitiveObjectInspector inputOI;
		StructObjectInspector structOI;
		StandardListObjectInspector listOI;
		StructField listField;
		Object[] partialResult;
		ListObjectInspector listFieldOI;

		@Override
		public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
			assert (parameters.length == 1);
			super.init(m, parameters);

			listOI = ObjectInspectorFactory
					.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableIntObjectInspector);
			// init input
			if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {//map阶段的时候，输入只是Primitive的一个数据而已
				//输入就是一个参数的Primitive类型的数据
				inputOI = (PrimitiveObjectInspector) parameters[0];
			} else {//reduce阶段的时候
				structOI = (StructObjectInspector) parameters[0];//输入是一个Struct结构体
				listField = structOI.getStructFieldRef("list");//Struct结构体里面有个list属性，设置该属性名为"list"
				listFieldOI = (ListObjectInspector) listField.getFieldObjectInspector();//得到list属性的解析器
			}

			// init output
			if (m == Mode.PARTIAL1 || m == Mode.PARTIAL2) {//在map和reduce阶段是需要有输出的
				ArrayList<ObjectInspector> foi = new ArrayList<ObjectInspector>();//属性定义表
				foi.add(listOI);
				ArrayList<String> fname = new ArrayList<String>();
				fname.add("list");
				partialResult = new Object[1];
				partialResult[0] = new ArrayList<IntWritable>();
				//里面放的就是listOI->list,后面通过属性名得到这个listOI
				return ObjectInspectorFactory.getStandardStructObjectInspector(fname, foi);
			} else {//在不需要输出的情况下直接是double类型的解析器
				return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
			}

		}

		static class MedianNumberAgg implements AggregationBuffer {
			List<IntWritable> aggIntegerList;
		}

		@Override
		public AggregationBuffer getNewAggregationBuffer() throws HiveException {
			MedianNumberAgg resultAgg = new MedianNumberAgg();
			reset(resultAgg);
			return resultAgg;
		}

		@Override
		public void reset(AggregationBuffer agg) throws HiveException {
			MedianNumberAgg medianNumberAgg = (MedianNumberAgg) agg;
			medianNumberAgg.aggIntegerList = null;
			medianNumberAgg.aggIntegerList = new ArrayList<IntWritable>();
		}

		boolean warned = false;

		@Override
		// map阶段，迭代处理输入sql传过来的列数据  
		public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
			assert (parameters.length == 1);
			if (parameters[0] != null) {
				MedianNumberAgg medianNumberAgg = (MedianNumberAgg) agg;
				int val = 0;
				try {
					val = PrimitiveObjectInspectorUtils.getInt(parameters[0], (PrimitiveObjectInspector) inputOI);
				} catch (NullPointerException e) {
					LOG.warn("got a null value, skip it");
				} catch (NumberFormatException e) {
					if (!warned) {
						warned = true;
						LOG.warn(getClass().getSimpleName() + " " + StringUtils.stringifyException(e));
						LOG.warn("ignore similar exceptions.");
					}

				}
				//降数据放在list中去
				medianNumberAgg.aggIntegerList.add(new IntWritable(val));
			}
		}

		@SuppressWarnings("unchecked")
		@Override
		// reducer阶段，输出最终结果  
		public Object terminate(AggregationBuffer agg) throws HiveException {
			//result就是double
			MedianNumberAgg medianNumberAgg = (MedianNumberAgg) agg;
			Collections.sort(medianNumberAgg.aggIntegerList);
			int size = medianNumberAgg.aggIntegerList.size();
			if (size == 1) {
				result.set((double) medianNumberAgg.aggIntegerList.get(0).get());
				return result;
			}
			double rs = 0.0;
			// int midIndex = (int) Math.floor(((double) size / 2));
			int midIndex = size / 2;
			if (size % 2 == 1) {
				rs = (double) medianNumberAgg.aggIntegerList.get(midIndex).get();
			} else if (size % 2 == 0) {
				rs = (medianNumberAgg.aggIntegerList.get(midIndex - 1).get()
						+ medianNumberAgg.aggIntegerList.get(midIndex).get()) / 2.0;
			}
			result.set(rs);
			return result;
		}

		@Override
		// map与combiner结束返回结果，得到部分数据聚集结果 
		public Object terminatePartial(AggregationBuffer agg) throws HiveException {
			MedianNumberAgg medianNumberAgg = (MedianNumberAgg) agg;
			//将部分的聚集结果放在partialResult中去，后面的merge会用到这个partialResult
			partialResult[0] = new ArrayList<IntWritable>(medianNumberAgg.aggIntegerList.size());
			((ArrayList<IntWritable>) partialResult[0]).addAll(medianNumberAgg.aggIntegerList);
			return partialResult;
		}

		@Override
		// combiner合并map返回的结果，还有reducer合并mapper或combiner返回的结果。 
		public void merge(AggregationBuffer agg, Object partial) throws HiveException {
			MedianNumberAgg medianNumberAgg = (MedianNumberAgg) agg;
			Object partialObject = structOI.getStructFieldData(partial, listField);
			ArrayList<IntWritable> resultList = (ArrayList<IntWritable>) listFieldOI.getList(partialObject);
			for (IntWritable i : resultList) {
				medianNumberAgg.aggIntegerList.add(i);
			}
		}

	}

	public static class GenericUDAFMedianEvaluatorDouble extends GenericUDAFEvaluator {

		private DoubleWritable result = new DoubleWritable();
		PrimitiveObjectInspector inputOI;
		StructObjectInspector structOI;
		StandardListObjectInspector listOI;
		StructField listField;
		Object[] partialResult;
		ListObjectInspector listFieldOI;

		@Override
		public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
			assert (parameters.length == 1);
			super.init(m, parameters);

			listOI = ObjectInspectorFactory
					.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
			// init input
			if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
				inputOI = (PrimitiveObjectInspector) parameters[0];
			} else {
				structOI = (StructObjectInspector) parameters[0];
				listField = structOI.getStructFieldRef("list");
				listFieldOI = (ListObjectInspector) listField.getFieldObjectInspector();
			}

			// init output
			if (m == Mode.PARTIAL1 || m == Mode.PARTIAL2) {
				ArrayList<ObjectInspector> foi = new ArrayList<ObjectInspector>();
				foi.add(listOI);
				ArrayList<String> fname = new ArrayList<String>();
				fname.add("list");
				partialResult = new Object[1];
				return ObjectInspectorFactory.getStandardStructObjectInspector(fname, foi);
			} else {
				return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
			}

		}

		static class MedianNumberAgg implements AggregationBuffer {
			List<DoubleWritable> aggIntegerList;
		}

		@Override
		public AggregationBuffer getNewAggregationBuffer() throws HiveException {
			MedianNumberAgg resultAgg = new MedianNumberAgg();
			reset(resultAgg);
			return resultAgg;
		}

		@Override
		public void reset(AggregationBuffer agg) throws HiveException {
			MedianNumberAgg medianNumberAgg = (MedianNumberAgg) agg;
			medianNumberAgg.aggIntegerList = null;
			medianNumberAgg.aggIntegerList = new ArrayList<DoubleWritable>();
		}

		boolean warned = false;

		@Override
		public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
			assert (parameters.length == 1);
			if (parameters[0] != null) {
				MedianNumberAgg medianNumberAgg = (MedianNumberAgg) agg;
				double val = 0.0;
				try {
					val = PrimitiveObjectInspectorUtils.getDouble(parameters[0], (PrimitiveObjectInspector) inputOI);
				} catch (NullPointerException e) {
					LOG.warn("got a null value, skip it");
				} catch (NumberFormatException e) {
					if (!warned) {
						warned = true;
						LOG.warn(getClass().getSimpleName() + " " + StringUtils.stringifyException(e));
						LOG.warn("ignore similar exceptions.");
					}

				}
				medianNumberAgg.aggIntegerList.add(new DoubleWritable(val));
			}
		}

		@SuppressWarnings("unchecked")
		@Override
		public Object terminate(AggregationBuffer agg) throws HiveException {
			MedianNumberAgg medianNumberAgg = (MedianNumberAgg) agg;
			Collections.sort(medianNumberAgg.aggIntegerList);
			int size = medianNumberAgg.aggIntegerList.size();
			if (size == 1) {
				result.set((double) medianNumberAgg.aggIntegerList.get(0).get());
				return result;
			}
			double rs = 0.0;
			// int midIndex = (int) Math.floor(((double) size / 2));
			int midIndex = size / 2;
			if (size % 2 == 1) {
				rs = (double) medianNumberAgg.aggIntegerList.get(midIndex).get();
			} else if (size % 2 == 0) {
				rs = (medianNumberAgg.aggIntegerList.get(midIndex - 1).get()
						+ medianNumberAgg.aggIntegerList.get(midIndex).get()) / 2.0;
			}
			result.set(rs);
			return result;
		}

		@Override
		public Object terminatePartial(AggregationBuffer agg) throws HiveException {
			MedianNumberAgg medianNumberAgg = (MedianNumberAgg) agg;
			partialResult[0] = new ArrayList<DoubleWritable>(medianNumberAgg.aggIntegerList.size());
			((ArrayList<DoubleWritable>) partialResult[0]).addAll(medianNumberAgg.aggIntegerList);
			return partialResult;
		}

		@Override
		public void merge(AggregationBuffer agg, Object partial) throws HiveException {
			MedianNumberAgg medianNumberAgg = (MedianNumberAgg) agg;
			Object partialObject = structOI.getStructFieldData(partial, listField);
			ArrayList<DoubleWritable> resultList = (ArrayList<DoubleWritable>) listFieldOI.getList(partialObject);
			for (DoubleWritable i : resultList) {
				medianNumberAgg.aggIntegerList.add(i);
			}
		}

	}

	public static class GenericUDAFMedianEvaluatorLong extends GenericUDAFEvaluator {

		private DoubleWritable result = new DoubleWritable();
		PrimitiveObjectInspector inputOI;
		StructObjectInspector structOI;
		StandardListObjectInspector listOI;
		StructField listField;
		Object[] partialResult;
		ListObjectInspector listFieldOI;

		@Override
		public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
			assert (parameters.length == 1);
			super.init(m, parameters);

			listOI = ObjectInspectorFactory
					.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableLongObjectInspector);
			// init input
			if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
				inputOI = (PrimitiveObjectInspector) parameters[0];
			} else {
				structOI = (StructObjectInspector) parameters[0];
				listField = structOI.getStructFieldRef("list");
				listFieldOI = (ListObjectInspector) listField.getFieldObjectInspector();
			}

			// init output
			if (m == Mode.PARTIAL1 || m == Mode.PARTIAL2) {
				ArrayList<ObjectInspector> foi = new ArrayList<ObjectInspector>();
				foi.add(listOI);
				ArrayList<String> fname = new ArrayList<String>();
				fname.add("list");
				partialResult = new Object[1];
				partialResult[0] = new ArrayList<LongWritable>();
				return ObjectInspectorFactory.getStandardStructObjectInspector(fname, foi);
			} else {
				return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
			}

		}

		static class MedianNumberAgg implements AggregationBuffer {
			List<LongWritable> aggIntegerList;
		}

		@Override
		public AggregationBuffer getNewAggregationBuffer() throws HiveException {
			MedianNumberAgg resultAgg = new MedianNumberAgg();
			reset(resultAgg);
			return resultAgg;
		}

		@Override
		public void reset(AggregationBuffer agg) throws HiveException {
			MedianNumberAgg medianNumberAgg = (MedianNumberAgg) agg;
			medianNumberAgg.aggIntegerList = null;
			medianNumberAgg.aggIntegerList = new ArrayList<LongWritable>();
		}

		boolean warned = false;

		@Override
		public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
			assert (parameters.length == 1);
			if (parameters[0] != null) {
				MedianNumberAgg medianNumberAgg = (MedianNumberAgg) agg;
				long val = 0L;
				try {
					val = PrimitiveObjectInspectorUtils.getLong(parameters[0], (PrimitiveObjectInspector) inputOI);
				} catch (NullPointerException e) {
					LOG.warn("got a null value, skip it");
				} catch (NumberFormatException e) {
					if (!warned) {
						warned = true;
						LOG.warn(getClass().getSimpleName() + " " + StringUtils.stringifyException(e));
						LOG.warn("ignore similar exceptions.");
					}

				}
				medianNumberAgg.aggIntegerList.add(new LongWritable(val));
			}
		}

		@SuppressWarnings("unchecked")
		@Override
		public Object terminate(AggregationBuffer agg) throws HiveException {
			MedianNumberAgg medianNumberAgg = (MedianNumberAgg) agg;
			Collections.sort(medianNumberAgg.aggIntegerList);
			int size = medianNumberAgg.aggIntegerList.size();
			if (size == 1) {
				result.set((double) medianNumberAgg.aggIntegerList.get(0).get());
				return result;
			}
			double rs = 0.0;
			// int midIndex = (int) Math.floor(((double) size / 2));
			int midIndex = size / 2;
			if (size % 2 == 1) {
				rs = (double) medianNumberAgg.aggIntegerList.get(midIndex).get();
			} else if (size % 2 == 0) {
				rs = (medianNumberAgg.aggIntegerList.get(midIndex - 1).get()
						+ medianNumberAgg.aggIntegerList.get(midIndex).get()) / 2.0;
			}
			result.set(rs);
			return result;
		}

		@Override
		public Object terminatePartial(AggregationBuffer agg) throws HiveException {
			MedianNumberAgg medianNumberAgg = (MedianNumberAgg) agg;
			partialResult[0] = new ArrayList<LongWritable>(medianNumberAgg.aggIntegerList.size());
			((ArrayList<LongWritable>) partialResult[0]).addAll(medianNumberAgg.aggIntegerList);
			return partialResult;
		}

		@Override
		public void merge(AggregationBuffer agg, Object partial) throws HiveException {
			MedianNumberAgg medianNumberAgg = (MedianNumberAgg) agg;
			Object partialObject = structOI.getStructFieldData(partial, listField);
			ArrayList<LongWritable> resultList = (ArrayList<LongWritable>) listFieldOI.getList(partialObject);
			for (LongWritable i : resultList) {
				medianNumberAgg.aggIntegerList.add(i);
			}
		}

	}
}
