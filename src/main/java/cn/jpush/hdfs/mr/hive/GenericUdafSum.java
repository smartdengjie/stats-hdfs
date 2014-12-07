/**
 * 
 */
package cn.jpush.hdfs.mr.hive;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author dengjie
 * @date 2014年12月5日
 * @description 编写hive的udaf来处理复杂的代码逻辑，本类处理一个带条件的Sum
 */
public class GenericUdafSum extends AbstractGenericUDAFResolver {

    private static final Logger log = LoggerFactory.getLogger(GenericUdafSum.class);

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver#
     * getEvaluator(org.apache.hadoop.hive.serde2.typeinfo.TypeInfo[])
     */
    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] info) throws SemanticException {
	// TODO Auto-generated method stub
	return new GenericUdafMeberLevelEvaluator();
    }

    public static class GenericUdafMeberLevelEvaluator extends GenericUDAFEvaluator {
	private PrimitiveObjectInspector inputOI;
	private PrimitiveObjectInspector inputOI2;
	private PrimitiveObjectInspector outputOI;
	private DoubleWritable result;

	@Override
	public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
	    // TODO Auto-generated method stub
	    super.init(m, parameters);

	    // 初始化input
	    if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
		log.info("Mode ->" + m.toString() + " result has init");
		inputOI = (PrimitiveObjectInspector) parameters[0];
		inputOI2 = (PrimitiveObjectInspector) parameters[1];
	    }

	    // 初始化output
	    if (m == Mode.PARTIAL2 || m == Mode.FINAL) {
		outputOI = (PrimitiveObjectInspector) parameters[0];
		result = new DoubleWritable(0);
		return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
	    } else {
		result = new DoubleWritable(0);
		return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
	    }
	}

	@SuppressWarnings("deprecation")
	public static class SumAgg implements AggregationBuffer {
	    boolean empty;
	    double value;
	}

	@SuppressWarnings("deprecation")
	@Override
	// 创建新的聚合计算的需要的内存，用来存储mapper，combiner，reduce运行过程中的相加总和。
	// 使用buffer对象钱，先进行内存的情况 -> reset
	public AggregationBuffer getNewAggregationBuffer() throws HiveException {
	    SumAgg buffer = new SumAgg();
	    reset(buffer);
	    return buffer;
	}

	@SuppressWarnings("deprecation")
	@Override
	// 重置0
	// mapreduce支持mapper和reduce的重启，所以为了兼容，也需要做内存的重用
	public void reset(AggregationBuffer agg) throws HiveException {
	    ((SumAgg) agg).value = 0.0;
	    ((SumAgg) agg).empty = true;
	}

	private boolean warnd = false;

	@SuppressWarnings("deprecation")
	@Override
	// 迭代
	// 只有保存当前的对象agg和输入的参数
	public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
	    if (parameters == null) {
		return;
	    }
	    try {
		double flag = PrimitiveObjectInspectorUtils.getDouble(parameters[1], inputOI2);
		if (flag > 1.0) {// 条件参数
		    merge(agg, parameters[0]);// 这里将迭代数据放入combiner进行合并
		}
	    } catch (Exception e) {
		e.printStackTrace();
		log.warn("warn -> " + e.getMessage());
	    }
	}

	@SuppressWarnings("deprecation")
	@Override
	public Object terminatePartial(AggregationBuffer agg) throws HiveException {
	    return null;
	}

	@SuppressWarnings("deprecation")
	@Override
	public void merge(AggregationBuffer agg, Object partial) throws HiveException {
	    if (partial != null) {
		// 通过ObjectInspecor取每一个字段的数据
		if (inputOI != null) {
		    double p = PrimitiveObjectInspectorUtils.getDouble(partial, inputOI);
		    log.info("add up 1 -> " + p);
		    ((SumAgg) agg).value += p;
		} else {
		    double p = PrimitiveObjectInspectorUtils.getDouble(partial, inputOI);
		    log.info("add up 2 -> " + p);
		    ((SumAgg) agg).value += p;
		}
	    }
	}

	@SuppressWarnings("deprecation")
	@Override
	public Object terminate(AggregationBuffer agg) throws HiveException {
	    SumAgg sumAgg = (SumAgg) agg;
	    result.set(sumAgg.value);
	    return result;
	}

    }

}
