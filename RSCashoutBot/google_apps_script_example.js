function doPost(e) {
  const CONFIG = {
    apiKey: 'CHANGE_ME',
    spreadsheetId: 'PASTE_SPREADSHEET_ID_HERE',
    routes: {
      signup_form: 'Signup Form'
    }
  };

  const headerKey = 'X-API-Key';
  const requestKey = e && e.parameter ? e.parameter[headerKey] : null;
  const body = JSON.parse(e.postData.contents || '{}');
  const providedKey = requestKey || body.apiKey || '';

  if (CONFIG.apiKey && providedKey !== CONFIG.apiKey) {
    return ContentService
      .createTextOutput(JSON.stringify({ ok: false, error: 'unauthorized' }))
      .setMimeType(ContentService.MimeType.JSON);
  }

  const route = body.route;
  const payload = body.payload || {};
  const sheetName = CONFIG.routes[route];
  if (!sheetName) {
    return ContentService
      .createTextOutput(JSON.stringify({ ok: false, error: 'unknown_route' }))
      .setMimeType(ContentService.MimeType.JSON);
  }

  const sheet = SpreadsheetApp.openById(CONFIG.spreadsheetId).getSheetByName(sheetName);
  if (!sheet) {
    return ContentService
      .createTextOutput(JSON.stringify({ ok: false, error: 'missing_sheet' }))
      .setMimeType(ContentService.MimeType.JSON);
  }

  sheet.appendRow([
    new Date(),
    payload.guild_id || '',
    payload.channel_id || '',
    payload.user_id || '',
    payload.username || '',
    payload.display_name || '',
    payload.ticket_type || '',
    payload.ticket_label || '',
    JSON.stringify(payload.values || {}),
    payload.created_at || ''
  ]);

  return ContentService
    .createTextOutput(JSON.stringify({ ok: true }))
    .setMimeType(ContentService.MimeType.JSON);
}
