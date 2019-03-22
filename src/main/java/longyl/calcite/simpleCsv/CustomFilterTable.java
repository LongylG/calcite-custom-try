package longyl.calcite.simpleCsv;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.FilterableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class CustomFilterTable extends AbstractTable implements FilterableTable {
    @Override
    public Enumerable<Object[]> scan(DataContext root, List<RexNode> filters) {
        return null;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        RelDataTypeFactory.Builder builder = typeFactory.builder();
        builder.add("name", SqlTypeName.VARCHAR);
        return builder.build();
    }
}
