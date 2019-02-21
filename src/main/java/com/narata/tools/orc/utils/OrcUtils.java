package com.narata.tools.orc.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcMapredRecordReader;
import org.apache.orc.mapred.OrcStruct;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author narata
 * @since 2019/02/21
 */
public class OrcUtils {
	/**
	 * 根据本地Orc文件返回 OrcStruct List
	 * @param filename 本地文件名
	 * @return List OrcStruct
	 * @throws IOException
	 */
	public static List<OrcStruct> localOrcFileToList(String filename) throws IOException {
		Path testFilePath = new Path(filename);
		Configuration conf = new Configuration();
		Reader reader = OrcFile.createReader(testFilePath, OrcFile.readerOptions(conf));
		RecordReader rows = reader.rows();
		TypeDescription schema = reader.getSchema();
		List<TypeDescription> children = schema.getChildren();
		VectorizedRowBatch batch = schema.createRowBatch();
		int numberOfChildren = children.size();
		List<OrcStruct> resultList = new ArrayList<>();
		while (rows.nextBatch(batch)) {
			for (int r = 0; r < batch.size; r++) {
				OrcStruct result = new OrcStruct(schema);
				for(int i=0; i < numberOfChildren; ++i) {
					result.setFieldValue(i, OrcMapredRecordReader.nextValue(batch.cols[i], 1,
							children.get(i), result.getFieldValue(i)));
				}
				resultList.add(result);
			}
		}
		rows.close();
		return resultList;
	}
}
