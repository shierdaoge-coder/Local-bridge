import { canonical, insist, PROTOCOL } from '../../src/protocol.mjs';

export function nativeTestScope(turns, currentStage) {
  return {
    checkHistory(archiveId, records, db) {
      const stage = currentStage();
      const approved = turns.filter(turn => turn.stage <= stage && turn.archive === archiveId);
      const sequence = db.prepare('SELECT record FROM indexed_records WHERE archive_id=? ORDER BY seq').all(archiveId)
        .map(row => JSON.parse(row.record)).concat(records);
      let offset = 0;
      for (const turn of approved) {
        for (const [role, text, optional] of [
          ['user', turn.prompt, false],
          ['assistant', turn.progress, true],
          ['assistant', turn.final, false],
        ]) {
          const record = sequence[offset];
          if (!record) return;
          if (record.role === role && record.text === text) offset++;
          else insist(optional, 'TEST_SCOPE_MISMATCH', 'Visible input differs from the approved synthetic turn');
        }
      }
      insist(offset === sequence.length, 'TEST_SCOPE_MISMATCH', 'Visible input exceeds the approved synthetic turns');
    },
    checkPublication(sender, input) {
      const stage = currentStage();
      const approved = turns.some(turn => turn.stage <= stage && turn.endpoint === sender && turn.event
        && canonical(input) === canonical({ protocol: PROTOCOL, content_type: 'message', ...turn.event }));
      insist(approved, 'TEST_SCOPE_MISMATCH', 'Publication differs from the approved synthetic event');
    },
  };
}
