import 'dart:io';

import 'package:args/args.dart';
import 'package:csv/csv.dart';
import 'package:path/path.dart' as p;
import 'package:postgres/postgres.dart';

class AppConfig {
  AppConfig({
    required this.dbHost,
    required this.dbPort,
    required this.dbName,
    required this.dbUser,
    required this.dbPassword,
    required this.schema,
  });

  final String dbHost;
  final int dbPort;
  final String dbName;
  final String dbUser;
  final String dbPassword;
  final String schema;

  static AppConfig fromEnv({required String schema}) {
    return AppConfig(
      dbHost: _requireEnv('PGHOST'),
      dbPort: int.tryParse(_requireEnv('PGPORT')) ?? 5432,
      dbName: _requireEnv('PGDATABASE'),
      dbUser: _requireEnv('PGUSER'),
      dbPassword: _requireEnv('PGPASSWORD'),
      schema: schema,
    );
  }

  static String _requireEnv(String key) {
    final value = Platform.environment[key];
    if (value == null || value.trim().isEmpty) {
      throw StateError('Missing required environment variable: $key');
    }
    return value.trim();
  }
}

class SnapshotOptions {
  SnapshotOptions({
    required this.source,
    required this.snapshotDate,
    required this.program,
    required this.notes,
  });

  final String source;
  final DateTime snapshotDate;
  final String program;
  final String notes;
}

String formatDate(DateTime date) => _formatDate(date);

DateTime parseDate(String value) => _parseDate(value);

String formatPercent({required int numerator, required int denominator}) {
  if (denominator == 0) {
    return '0.0';
  }
  final value = (numerator / denominator) * 100;
  return value.toStringAsFixed(1);
}

class ReportOptions {
  ReportOptions({required this.snapshotDate, required this.outputPath});

  final DateTime snapshotDate;
  final String outputPath;
}

class SummaryOptions {
  SummaryOptions({
    required this.snapshotDate,
    required this.outputPath,
    required this.staleDays,
  });

  final DateTime snapshotDate;
  final String outputPath;
  final int staleDays;
}

class CohortRecord {
  CohortRecord({
    required this.scholarId,
    required this.fullName,
    required this.program,
    required this.status,
    required this.touchpointStatus,
    required this.lastTouchpoint,
    required this.riskLevel,
    required this.engagementScore,
  });

  final String scholarId;
  final String fullName;
  final String program;
  final String status;
  final String touchpointStatus;
  final DateTime? lastTouchpoint;
  final String riskLevel;
  final int engagementScore;
}

Future<void> run(List<String> arguments) async {
  final parser = _buildParser();
  late ArgResults results;
  try {
    results = parser.parse(arguments);
  } on ArgParserException catch (error) {
    stderr.writeln(error.message);
    stderr.writeln(parser.usage);
    exitCode = 64;
    return;
  }

  if (results['help'] as bool) {
    stdout.writeln(parser.usage);
    return;
  }

  final schema = results['schema'] as String;
  try {
    final config = AppConfig.fromEnv(schema: schema);
    if (results.command?.name == 'init') {
      await _initDatabase(config);
      stdout.writeln('Initialized schema ${config.schema}.');
      return;
    }

    if (results.command?.name == 'snapshot') {
      final opts = _snapshotOptions(results.command!);
      await _captureSnapshot(config, opts);
      stdout.writeln(
        'Captured snapshot ${_formatDate(opts.snapshotDate)} for ${opts.program}.',
      );
      return;
    }

    if (results.command?.name == 'report') {
      final opts = _reportOptions(results.command!);
      await _generateReport(config, opts);
      stdout.writeln('Report written to ${opts.outputPath}.');
      return;
    }

    if (results.command?.name == 'summary') {
      final opts = _summaryOptions(results.command!);
      await _generateSummaryReport(config, opts);
      if (opts.outputPath != '-') {
        stdout.writeln('Summary report written to ${opts.outputPath}.');
      }
      return;
    }

    stdout.writeln(parser.usage);
  } catch (error) {
    stderr.writeln('Error: $error');
    exitCode = 1;
  }
}

ArgParser _buildParser() {
  final parser = ArgParser()
    ..addOption(
      'schema',
      defaultsTo: 'cohort_snapshotter',
      help: 'Postgres schema to use for this project.',
    )
    ..addFlag('help', abbr: 'h', negatable: false, help: 'Show help.');

  final init = parser.addCommand('init');
  init.addFlag('drop', defaultsTo: false, help: 'Drop existing tables.');

  final snapshot = parser.addCommand('snapshot');
  snapshot
    ..addOption(
      'source',
      defaultsTo: 'manual',
      help: 'Data source tag (e.g. airtable, export).',
    )
    ..addOption(
      'date',
      defaultsTo: _formatDate(DateTime.now()),
      help: 'Snapshot date (YYYY-MM-DD).',
    )
    ..addOption(
      'program',
      defaultsTo: 'Foundations',
      help: 'Program or cohort name.',
    )
    ..addOption('notes', defaultsTo: '', help: 'Optional snapshot notes.');

  final report = parser.addCommand('report');
  report
    ..addOption(
      'date',
      defaultsTo: _formatDate(DateTime.now()),
      help: 'Snapshot date (YYYY-MM-DD).',
    )
    ..addOption(
      'out',
      defaultsTo: p.join('reports', 'cohort_snapshot_report.csv'),
      help: 'Output CSV path.',
    );

  final summary = parser.addCommand('summary');
  summary
    ..addOption(
      'date',
      defaultsTo: _formatDate(DateTime.now()),
      help: 'Snapshot date (YYYY-MM-DD).',
    )
    ..addOption(
      'out',
      defaultsTo: p.join('reports', 'cohort_summary_report.csv'),
      help: 'Output CSV path, or - for stdout.',
    )
    ..addOption(
      'stale-days',
      defaultsTo: '14',
      help: 'Days since last touchpoint to count as stale.',
    );

  return parser;
}

SnapshotOptions _snapshotOptions(ArgResults results) {
  final date = _parseDate(results['date'] as String);
  return SnapshotOptions(
    source: results['source'] as String,
    snapshotDate: date,
    program: results['program'] as String,
    notes: results['notes'] as String,
  );
}

ReportOptions _reportOptions(ArgResults results) {
  final date = _parseDate(results['date'] as String);
  return ReportOptions(
    snapshotDate: date,
    outputPath: results['out'] as String,
  );
}

SummaryOptions _summaryOptions(ArgResults results) {
  final date = _parseDate(results['date'] as String);
  final staleDays = int.tryParse(results['stale-days'] as String) ?? 14;
  return SummaryOptions(
    snapshotDate: date,
    outputPath: results['out'] as String,
    staleDays: staleDays,
  );
}

Future<void> _initDatabase(AppConfig config) async {
  final conn = await _openConnection(config);
  try {
    await conn.execute('CREATE SCHEMA IF NOT EXISTS ${config.schema};');
    await conn.execute('SET search_path TO ${config.schema};');
    await conn.execute('''
CREATE TABLE IF NOT EXISTS cohort_snapshots (
  id SERIAL PRIMARY KEY,
  snapshot_date DATE NOT NULL,
  program TEXT NOT NULL,
  source TEXT NOT NULL,
  notes TEXT DEFAULT '',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);''');
    await conn.execute('''
CREATE TABLE IF NOT EXISTS cohort_members (
  id SERIAL PRIMARY KEY,
  snapshot_id INTEGER NOT NULL REFERENCES cohort_snapshots(id) ON DELETE CASCADE,
  scholar_id TEXT NOT NULL,
  full_name TEXT NOT NULL,
  status TEXT NOT NULL,
  touchpoint_status TEXT NOT NULL,
  last_touchpoint DATE,
  risk_level TEXT NOT NULL,
  engagement_score INTEGER NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);''');
    await conn.execute('''
CREATE INDEX IF NOT EXISTS idx_cohort_members_snapshot
  ON cohort_members(snapshot_id);''');
  } finally {
    await conn.close();
  }
}

Future<void> _captureSnapshot(AppConfig config, SnapshotOptions options) async {
  final conn = await _openConnection(config);
  try {
    await conn.execute('SET search_path TO ${config.schema};');
    final snapshotId = await conn.execute(
      Sql.named('''
INSERT INTO cohort_snapshots (snapshot_date, program, source, notes)
VALUES (@date, @program, @source, @notes)
RETURNING id;'''),
      parameters: {
        'date': options.snapshotDate,
        'program': options.program,
        'source': options.source,
        'notes': options.notes,
      },
    );
    final id = snapshotId.first.first as int;
    final records = _seedRecords(
      program: options.program,
      snapshotDate: options.snapshotDate,
    );
    for (final record in records) {
      await conn.execute(
        Sql.named('''
INSERT INTO cohort_members (
  snapshot_id,
  scholar_id,
  full_name,
  status,
  touchpoint_status,
  last_touchpoint,
  risk_level,
  engagement_score
)
VALUES (
  @snapshotId,
  @scholarId,
  @fullName,
  @status,
  @touchpointStatus,
  @lastTouchpoint,
  @riskLevel,
  @engagementScore
);'''),
        parameters: {
          'snapshotId': id,
          'scholarId': record.scholarId,
          'fullName': record.fullName,
          'status': record.status,
          'touchpointStatus': record.touchpointStatus,
          'lastTouchpoint': record.lastTouchpoint,
          'riskLevel': record.riskLevel,
          'engagementScore': record.engagementScore,
        },
      );
    }
  } finally {
    await conn.close();
  }
}

Future<void> _generateReport(AppConfig config, ReportOptions options) async {
  final conn = await _openConnection(config);
  try {
    await conn.execute('SET search_path TO ${config.schema};');
    final rows = await conn.execute(
      Sql.named('''
SELECT s.snapshot_date,
       s.program,
       s.source,
       s.notes,
       m.scholar_id,
       m.full_name,
       m.status,
       m.touchpoint_status,
       m.last_touchpoint,
       m.risk_level,
       m.engagement_score
FROM cohort_snapshots s
JOIN cohort_members m ON s.id = m.snapshot_id
WHERE s.snapshot_date = @date
ORDER BY s.program, m.full_name;'''),
      parameters: {'date': options.snapshotDate},
    );

    final data = <List<dynamic>>[
      [
        'snapshot_date',
        'program',
        'source',
        'notes',
        'scholar_id',
        'full_name',
        'status',
        'touchpoint_status',
        'last_touchpoint',
        'risk_level',
        'engagement_score',
      ],
    ];

    for (final row in rows) {
      data.add([
        _formatDate(row[0] as DateTime),
        row[1],
        row[2],
        row[3],
        row[4],
        row[5],
        row[6],
        row[7],
        row[8] == null ? '' : _formatDate(row[8] as DateTime),
        row[9],
        row[10],
      ]);
    }

    final csv = const ListToCsvConverter().convert(data);
    final file = File(options.outputPath);
    await file.parent.create(recursive: true);
    await file.writeAsString(csv);
  } finally {
    await conn.close();
  }
}

Future<void> _generateSummaryReport(
  AppConfig config,
  SummaryOptions options,
) async {
  final conn = await _openConnection(config);
  try {
    await conn.execute('SET search_path TO ${config.schema};');
    final staleDate = options.snapshotDate.subtract(
      Duration(days: options.staleDays),
    );
    final rows = await conn.execute(
      Sql.named('''
SELECT s.snapshot_date,
       s.program,
       COUNT(*) AS total_members,
       SUM(CASE WHEN m.status = 'Active' THEN 1 ELSE 0 END) AS active_members,
       SUM(
         CASE WHEN m.touchpoint_status = 'Needs Follow-Up'
         THEN 1 ELSE 0 END
       ) AS needs_followup_members,
       SUM(CASE WHEN m.risk_level = 'High' THEN 1 ELSE 0 END) AS high_risk,
       SUM(CASE WHEN m.risk_level = 'Medium' THEN 1 ELSE 0 END) AS medium_risk,
       SUM(CASE WHEN m.risk_level = 'Low' THEN 1 ELSE 0 END) AS low_risk,
       AVG(m.engagement_score) AS avg_engagement,
       SUM(
         CASE
           WHEN m.last_touchpoint IS NULL THEN 1
           WHEN m.last_touchpoint <= @staleDate THEN 1
           ELSE 0
         END
       ) AS stale_touchpoints
FROM cohort_snapshots s
JOIN cohort_members m ON s.id = m.snapshot_id
WHERE s.snapshot_date = @date
GROUP BY s.snapshot_date, s.program
ORDER BY s.program;
'''),
      parameters: {'date': options.snapshotDate, 'staleDate': staleDate},
    );

    final data = <List<dynamic>>[
      [
        'snapshot_date',
        'program',
        'total_members',
        'active_members',
        'needs_followup_members',
        'high_risk_members',
        'medium_risk_members',
        'low_risk_members',
        'avg_engagement_score',
        'stale_touchpoints',
        'pct_needs_followup',
        'pct_high_risk',
      ],
    ];

    for (final row in rows) {
      final total = (row[2] as int?) ?? 0;
      final needsFollowUp = (row[4] as int?) ?? 0;
      final highRisk = (row[5] as int?) ?? 0;
      final avgEngagement = row[8] as double?;
      data.add([
        _formatDate(row[0] as DateTime),
        row[1],
        total,
        row[3],
        needsFollowUp,
        highRisk,
        row[6],
        row[7],
        avgEngagement == null ? '' : avgEngagement.toStringAsFixed(1),
        row[9],
        formatPercent(numerator: needsFollowUp, denominator: total),
        formatPercent(numerator: highRisk, denominator: total),
      ]);
    }

    final csv = const ListToCsvConverter().convert(data);
    if (options.outputPath == '-') {
      stdout.write(csv);
      return;
    }
    final file = File(options.outputPath);
    await file.parent.create(recursive: true);
    await file.writeAsString(csv);
  } finally {
    await conn.close();
  }
}

Future<Connection> _openConnection(AppConfig config) async {
  final endpoint = Endpoint(
    host: config.dbHost,
    port: config.dbPort,
    database: config.dbName,
    username: config.dbUser,
    password: config.dbPassword,
  );
  return Connection.open(
    endpoint,
    settings: const ConnectionSettings(sslMode: SslMode.disable),
  );
}

List<CohortRecord> _seedRecords({
  required String program,
  required DateTime snapshotDate,
}) {
  final snapshot = snapshotDate;
  final roster = [
    _record(
      'GS-001',
      'Alina Booker',
      program,
      'Active',
      'On Track',
      snapshot.subtract(const Duration(days: 3)),
      'Low',
      92,
    ),
    _record(
      'GS-002',
      'Mateo Alvarez',
      program,
      'Active',
      'Needs Follow-Up',
      snapshot.subtract(const Duration(days: 12)),
      'Medium',
      71,
    ),
    _record(
      'GS-003',
      'Priya Shah',
      program,
      'Active',
      'Escalated',
      snapshot.subtract(const Duration(days: 20)),
      'High',
      58,
    ),
    _record(
      'GS-004',
      'Jordan Kim',
      program,
      'Leave of Absence',
      'Paused',
      snapshot.subtract(const Duration(days: 30)),
      'Medium',
      63,
    ),
    _record(
      'GS-005',
      'Sofia Ramirez',
      program,
      'Active',
      'On Track',
      snapshot.subtract(const Duration(days: 5)),
      'Low',
      88,
    ),
    _record(
      'GS-006',
      'Jalen Morris',
      program,
      'Active',
      'Needs Follow-Up',
      snapshot.subtract(const Duration(days: 15)),
      'High',
      61,
    ),
    _record(
      'GS-007',
      'Noor Hassan',
      program,
      'Active',
      'On Track',
      snapshot.subtract(const Duration(days: 2)),
      'Low',
      95,
    ),
    _record(
      'GS-008',
      'Dante Brooks',
      program,
      'Alumni',
      'Completed',
      snapshot.subtract(const Duration(days: 40)),
      'Low',
      78,
    ),
    _record(
      'GS-009',
      'Marisol Vega',
      program,
      'Active',
      'Needs Follow-Up',
      snapshot.subtract(const Duration(days: 9)),
      'Medium',
      74,
    ),
    _record(
      'GS-010',
      'Theo Nwosu',
      program,
      'Active',
      'On Track',
      snapshot.subtract(const Duration(days: 1)),
      'Low',
      97,
    ),
  ];
  return roster;
}

CohortRecord _record(
  String scholarId,
  String fullName,
  String program,
  String status,
  String touchpointStatus,
  DateTime? lastTouchpoint,
  String riskLevel,
  int engagementScore,
) {
  return CohortRecord(
    scholarId: scholarId,
    fullName: fullName,
    program: program,
    status: status,
    touchpointStatus: touchpointStatus,
    lastTouchpoint: lastTouchpoint,
    riskLevel: riskLevel,
    engagementScore: engagementScore,
  );
}

DateTime _parseDate(String value) {
  final parts = value.split('-');
  if (parts.length != 3) {
    throw FormatException('Invalid date format: $value');
  }
  return DateTime(
    int.parse(parts[0]),
    int.parse(parts[1]),
    int.parse(parts[2]),
  );
}

String _formatDate(DateTime date) {
  final month = date.month.toString().padLeft(2, '0');
  final day = date.day.toString().padLeft(2, '0');
  return '${date.year}-$month-$day';
}
