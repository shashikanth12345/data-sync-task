package app;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import app.config.*;

@Component
public class DataSyncTasks {
    @Autowired
    JdbcTemplate jdbcTemplate;

    @Autowired
    SyncConfig syncConfig;

    private static final Logger log = LoggerFactory.getLogger(DataSyncTasks.class);

    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    @Value("#{'${source-schemas}'.split(',')}")
    private List<String> sourceSchemas;

    @Value("${destination-schema}")
    private String destinationSchema;

    @Value("${state}")
    private String state;

    @Scheduled(fixedRate = 5000)
    public void startSync() {
        Timestamp epoch = findEpoch();
        String now = dateFormat.format(new Date());
        log.info("Staring sync at {}", now);

        for (String sourceSchema : sourceSchemas) {
            for (SyncInfo info : syncConfig.getInfo()) {
                String query = String.format("SELECT %s from %s.%s WHERE lastmodifieddate >=?",
                        String.join(",", getSourceColumns(info.getColumns())), sourceSchema, info.getSourceTable());

                jdbcTemplate.query(
                        query, new Object[]{epoch},
                        (rs, rowNum) -> new CustomResultSet(rs, info.getColumns())
                ).forEach(res -> {
                    try {
                        setDestinationSchema(sourceSchema, res);
                        insertOrUpdate(info, res, sourceSchema);
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                });
            }
        }
        updateEpoch(now);
    }

    private void setDestinationSchema(String sourceSchema, CustomResultSet rs) {
        if (Objects.equals(sourceSchema, "microservice")) {
            destinationSchema = String.format("%s.%s", state, rs.get("tenantId"));
        }
    }

    private List<String> getSourceColumns(List<ColumnConfig> columns) {
        return columns.stream().filter(ColumnConfig::isShouldSource).map(ColumnConfig::getSource).collect(Collectors.toList());
    }

    private List<String> getDestinationColumns(List<ColumnConfig> columns) {
        return columns.stream().filter(ColumnConfig::isShouldSync).map(ColumnConfig::getDestination).collect(Collectors.toList());
    }

    private Timestamp findEpoch() {
        List<Map<String, Object>> res = jdbcTemplate.queryForList("SELECT epoch from data_sync_epoch LIMIT 1");
        return (Timestamp) res.get(0).get("epoch");
    }

    private void updateEpoch(String epoch) {
        jdbcTemplate.update("UPDATE data_sync_epoch set epoch=?", new Object[]{Timestamp.valueOf(epoch)});
    }

    private void insertOrUpdate(SyncInfo info, CustomResultSet rs, String sourceSchema) throws SQLException {
        List<String> destinationColumns = getDestinationColumns(info.getColumns());
        ArrayList<String> destinationValues = getValuesFromResult(info, rs);
        if (Objects.equals(destinationSchema, "microservice")) {
            destinationColumns.add("tenantId");
            destinationValues.add(String.format("%s.%s", state, sourceSchema));
        }

        String insertQuery = String.format("INSERT INTO %s.%s (%s) VALUES (%s)",
                destinationSchema,
                info.getDestinationTable(),
                String.join(",", getDestinationColumns(info.getColumns())),
                String.join(",", getValuesFromResult(info, rs))
        );
        log.info("Trying to insert");
        log.info(insertQuery);
        try {
            jdbcTemplate.update(insertQuery);
        } catch (DuplicateKeyException e) {
            log.info("Insert failed");
            log.info("Trying to update");
            String updateQuery = String.format("UPDATE %s.%s set %s WHERE id = %s",
                    destinationSchema,
                    info.getDestinationTable(),
                    String.join(",", getColumnNameValuesFromResult(info, rs)),
                    rs.get("id")
            );
            log.info(updateQuery);
            jdbcTemplate.update(updateQuery);
        }
    }

    private ArrayList<String> getValuesFromResult(SyncInfo info, CustomResultSet rs) throws SQLException {
        ArrayList<String> values = new ArrayList<>();
        for (ColumnConfig column : info.getColumns()) {
            if (column.isShouldSync()) {
                String value;
                if (column.isShouldSource()) {
                    value = String.format("%s",rs.get(column.getSource()));
                } else {
                    value = column.getDefaultValue();
                }

                values.add((String.format("'%s'", value)));
            }
        }
        return values;
    }

    private ArrayList<String> getColumnNameValuesFromResult(SyncInfo info, CustomResultSet rs) throws SQLException {
        ArrayList<String> values = new ArrayList<>();
        for (ColumnConfig column : info.getColumns()) {
            if (column.isShouldSync()) {
                String value;
                if (column.isShouldSource()) {
                    value = String.format("%s",rs.get(column.getSource()));
                } else {
                    value = column.getDefaultValue();
                }
                values.add(String.format("%s = '%s'", column.getDestination(), value));
            }
        }
        return values;
    }
}
