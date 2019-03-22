package longyl.calcite.simpleCsv;

import com.google.common.collect.ImmutableMap;

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.util.Source;
import org.apache.calcite.util.Sources;

import java.net.URL;
import java.util.Map;

/**
 * 类似数据库，Schema表示数据库
 * */
public class CustomSchema extends AbstractSchema {
    private Map<String, Table> tableMap;

    @Override
    protected Map<String, Table> getTableMap() {
        URL url = CustomSchema.class.getResource("/data.csv");
        Source source = Sources.of(url);

        URL url2 = CustomSchema.class.getResource("/teacher.csv");
        Source source2 = Sources.of(url2);
        if (tableMap == null) {
            final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();
            builder.put("STUDENT",new CustomScannableTable(source));    // table name 大小写严格
            builder.put("TEACHER",new CustomScannableTable(source2));
            tableMap = builder.build();
        }
        return tableMap;
    }
}