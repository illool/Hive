package com.matthewrathbone.example;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableIntObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

public class GenericUDAFTopNRow extends AbstractGenericUDAFResolver {

	@Override
	public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
		if (parameters.length < 2) {
			throw new UDFArgumentTypeException(parameters.length - 1, "At least two argument is expected.");
		}

		if (!(TypeInfoUtils
				.getStandardWritableObjectInspectorFromTypeInfo(parameters[0]) instanceof WritableIntObjectInspector)) {
			throw new UDFArgumentTypeException(0, "The first argument must be integer,"
					+ TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(parameters[0]).getClass());
		}
		if (!ObjectInspectorUtils
				.compareSupported(TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(parameters[1]))) {
			throw new UDFArgumentTypeException(1,
					"Cannot support comparison of map<> type or complex type containing map<>.");
		}

		return new TopNEvaluator();
	}

	static class TopNBuffer implements AggregationBuffer {
		List<Object[]> container;
	}

	public static class TopNEvaluator extends GenericUDAFEvaluator {
		int size;
		String[] fieldNM;
		ObjectInspector[] fieldOI;
		ObjectInspector[] originalOI;
		StandardListObjectInspector partialOI;
		StandardStructObjectInspector partialElemOI;

		@Override
		//ObjectInspector用来帮助用户序列化和反序列化对象
		public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
			super.init(m, parameters);
			if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {//map阶段不需要输出流得初始化
				//有多少个parameters就应该有多少个ObjectInspector
				this.originalOI = new ObjectInspector[parameters.length];
				System.arraycopy(parameters, 0, this.originalOI, 0, parameters.length);
				//有多少个参数对象
				this.size = parameters.length - 1;
				//有多少个参数就有多少个域名
				this.fieldNM = new String[this.size];
				//有多少个参数就有多少个参数名字被记录
				this.fieldOI = new ObjectInspector[this.size];
				for (int i = 0; i < this.size; i++) {
					//给域名赋值
					this.fieldNM[i] = "f" + i;
					//参数名字赋值
					this.fieldOI[i] = ObjectInspectorUtils.getStandardObjectInspector(parameters[i + 1]);
				}
				return ObjectInspectorFactory.getStandardListObjectInspector(ObjectInspectorFactory
						.getStandardStructObjectInspector(Arrays.asList(this.fieldNM), Arrays.asList(this.fieldOI)));
			} else if (m == Mode.PARTIAL2 || m == Mode.FINAL) {//主要是要考虑输出流得初始化
				this.partialOI = (StandardListObjectInspector) parameters[0];
				this.partialElemOI = (StandardStructObjectInspector) this.partialOI.getListElementObjectInspector();
				List<? extends StructField> structFieldRefs = this.partialElemOI.getAllStructFieldRefs();
				this.size = structFieldRefs.size();
				this.fieldNM = new String[this.size];
				this.fieldOI = new ObjectInspector[this.size];
				for (int i = 0; i < this.size; i++) {
					StructField sf = structFieldRefs.get(i);
					this.fieldNM[i] = sf.getFieldName();
					this.fieldOI[i] = sf.getFieldObjectInspector();
				}
				return ObjectInspectorUtils.getStandardObjectInspector(this.partialOI);
			}

			return null;

		}

		@Override
		public AggregationBuffer getNewAggregationBuffer() throws HiveException {
			TopNBuffer buffer = new TopNBuffer();
			reset(buffer);
			return buffer;
		}

		@Override
		public void reset(AggregationBuffer agg) throws HiveException {
			TopNBuffer buffer = (TopNBuffer) agg;
			buffer.container = new LinkedList<Object[]>();
		}

		@Override
		public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
			if (isEmptySet(agg, parameters)) {
				return;
			}
			TopNBuffer buffer = (TopNBuffer) agg;
			//有n个值应该放在里面
			int n = ((WritableIntObjectInspector) this.originalOI[0]).get(parameters[0]);
			int s = buffer.container.size();//现在有s个
			if (s < n) {//要是不够n个
				Object[] elemVal = new Object[this.size];
				for (int j = 0; j < this.size; j++) {
					elemVal[j] = ObjectInspectorUtils.copyToStandardObject(parameters[j + 1], this.originalOI[j + 1]);
				}
				buffer.container.add(elemVal);
				/* make sure the size should be n */
				while (buffer.container.size() < n) {
					buffer.container.add(new Object[this.size]);
				}
			} else {
				for (int i = 0; i < s; i++) {
					if (ObjectInspectorUtils.compare(buffer.container.get(i)[0], this.fieldOI[0], parameters[1],
							this.originalOI[1]) < 0) {
						Object[] elemVal = new Object[this.size];
						for (int j = 0; j < this.size; j++) {
							elemVal[j] = ObjectInspectorUtils.copyToStandardObject(parameters[j + 1],
									this.originalOI[j + 1]);
						}
						buffer.container.add(i, elemVal);
						break;
					}
				}
				/* make sure the size should be n */
				while (buffer.container.size() > n) {
					buffer.container.remove(n);
				}
			}
		}

		@Override
		public Object terminatePartial(AggregationBuffer agg) throws HiveException {
			TopNBuffer buffer = (TopNBuffer) agg;
			return buffer.container.isEmpty() ? null : buffer.container;
		}

		@Override
		public void merge(AggregationBuffer agg, Object partial) {
			/* 如果查询结果为空,不作处理 */
			if (isEmptySet(agg, partial)) {
				return;
			}
			TopNBuffer buffer = (TopNBuffer) agg;
			List<?> listVal = this.partialOI.getList(partial);
			final int cn = Math.max(buffer.container.size(), listVal.size());
			List<Object[]> values = new LinkedList<Object[]>();
			for (Object elemObj : listVal) {
				List<Object> elemVal = this.partialElemOI.getStructFieldsDataAsList(elemObj);
				Object[] value = new Object[this.size];
				for (int i = 0, n = elemVal.size(); i < n; i++) {
					value[i] = ObjectInspectorUtils.copyToStandardObject(elemVal.get(i), this.fieldOI[i]);
				}
				values.add(value);
			}
			buffer.container = mergeSortNotNull(buffer.container, values);
			while (buffer.container.size() < cn) {
				buffer.container.add(new Object[this.size]);
			}
			while (buffer.container.size() > cn) {
				buffer.container.remove(cn);
			}
		}

		@Override
		public Object terminate(AggregationBuffer agg) throws HiveException {
			TopNBuffer buffer = (TopNBuffer) agg;
			return buffer.container.isEmpty() ? null : buffer.container;
		}

		private List<Object[]> mergeSortNotNull(List<Object[]> list1, List<Object[]> list2) {
			List<Object[]> result = new LinkedList<Object[]>();
			int i1 = 0, i2 = 0, n1 = list1.size(), n2 = list2.size();
			while (i1 < n1 && i2 < n2) {
				if (list1.get(i1)[0] == null) {
					i1++;
					continue;
				}
				if (list2.get(i2)[0] == null) {
					i2++;
					continue;
				}
				int cp = ObjectInspectorUtils.compare(list1.get(i1)[0], this.fieldOI[0], list2.get(i2)[0],
						this.fieldOI[0]);
				if (cp > 0) {
					result.add(list1.get(i1));
					i1++;
				} else if (cp < 0) {
					result.add(list2.get(i2));
					i2++;
				} else {
					result.add(list1.get(i1));
					i1++;
					i2++;
				}
			}
			while (i1 < n1) {
				if (list1.get(i1)[0] == null) {
					i1++;
					continue;
				}
				result.add(list1.get(i1));
				i1++;
			}
			while (i2 < n2) {
				if (list2.get(i2)[0] == null) {
					i2++;
					continue;
				}
				result.add(list2.get(i2));
				i2++;
			}
			return result;
		}

		private boolean isEmptySet(AggregationBuffer agg, Object[] parameters) {
			if (agg == null || parameters == null) {
				return true;
			} else {
				for (int i = 0; i < parameters.length; i++) {
					if (parameters[i] != null) {
						return false;
					}
				}
				return true;
			}
		}

		private boolean isEmptySet(AggregationBuffer agg, Object parameter) {
			return (agg == null) || (parameter == null);
		}
	}

}
