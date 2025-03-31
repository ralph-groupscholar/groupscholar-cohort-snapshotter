import 'package:groupscholar_cohort_snapshotter/groupscholar_cohort_snapshotter.dart';
import 'package:test/test.dart';

void main() {
  test('formatDate returns YYYY-MM-DD', () {
    final date = DateTime(2026, 2, 8);
    expect(formatDate(date), '2026-02-08');
  });

  test('parseDate reads YYYY-MM-DD', () {
    final date = parseDate('2026-01-15');
    expect(date.year, 2026);
    expect(date.month, 1);
    expect(date.day, 15);
  });

  test('formatPercent returns one decimal', () {
    expect(formatPercent(numerator: 1, denominator: 4), '25.0');
  });

  test('formatPercent handles zero denominator', () {
    expect(formatPercent(numerator: 3, denominator: 0), '0.0');
  });
}
