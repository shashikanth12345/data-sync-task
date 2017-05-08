package app;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import app.config.*;

public class CustomResultSet {
    private Map resultMap = new HashMap();
    private List<ColumnConfig> columnConfigs;
    private ResultSet rs;

    public CustomResultSet(ResultSet rs, List<ColumnConfig> columnConfigs) throws SQLException {
        this.rs = rs;
        this.columnConfigs = columnConfigs;
        fillResultMap();
    }

    private void fillResultMap() throws SQLException {
        for (ColumnConfig columnConfig : columnConfigs) {
            resultMap.put(columnConfig.getSource(), get(columnConfig.getSource(), columnConfig.getType()));
        }
    }

    public boolean next() throws SQLException {
        return rs.next();
    }

    public Object get(String columnLabel, String type) throws SQLException {
        Object value;
        switch (type) {
            case "Integer":
                value = this.rs.getLong(columnLabel);
                break;
            case "String":
                value = this.rs.getString(columnLabel);
                break;
            default:
                throw new SQLException(String.format("Column type: %s is not valid", type));
        }
        return value;
    }

    public Object get(String columnLabel) {
        return resultMap.get(columnLabel);
    }

    public ArrayList<String> getValuesToInsert(SyncInfo info) throws SQLException {
        ArrayList<String> values = new ArrayList<>();
        for (ColumnConfig column : info.getColumns()) {
            if (column.isShouldSync()) {
                String value;
                if (column.isShouldSource()) {
                    value = String.format("%s", get(column.getSource()));
                } else {
                    value = column.getDefaultValue();
                }

                values.add((String.format("'%s'", value)));
            }
        }
        return values;
    }
}
