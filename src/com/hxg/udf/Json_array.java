package com.hxg.udf;

import java.util.ArrayList;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

/**
 * 对hive中的json数组进行解析的函数
 * @author huixingguo
 *
 */
public class Json_array extends GenericUDF {

	private transient ObjectInspectorConverters.Converter converter;
	
	@Override
	public Object evaluate(DeferredObject[] arguments) throws HiveException {
		assert(arguments.length == 1);
		if(arguments[0].get() == null) {
			return null;
		}
		String jsonArrayStr = ((Text) converter.convert(arguments[0].get())).toString();
		if (StringUtils.isNotEmpty(jsonArrayStr)) {
			try {
				JsonParser jsonParser = new JsonParser();
				
				JsonElement jsonElement = jsonParser.parse(jsonArrayStr);
				
				if(jsonElement.isJsonArray()) {
					JsonArray jsonArray = jsonElement.getAsJsonArray();
					ArrayList<Text> result = new ArrayList<Text>();
					
					for(JsonElement je : jsonArray) {
						if(je.isJsonArray() || je.isJsonObject()) {
							result.add(new Text(je.toString()));
						}else if(je.isJsonPrimitive()) {
							result.add(new Text(je.getAsString()));
						}else if(je.isJsonNull()) {
							// ignore
						}
					}
					return result;
				}
			}catch (Exception e) {
				e.printStackTrace();
			}
		}
		return null;
	}

	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
		if(arguments.length != 1) {
			throw new UDFArgumentLengthException("The function json_array(jsonArrayStr) takes exactly 1 arguments");
		}
		converter = ObjectInspectorConverters.getConverter(arguments[0], PrimitiveObjectInspectorFactory.writableStringObjectInspector);
		
		return ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
	}

	@Override
	public String getDisplayString(String[] arg0) {
		// TODO Auto-generated method stub
		return null;
	}
	
}
