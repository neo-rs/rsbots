function doPost(e) {
  const CONFIG = {
    apiKey: "CHANGE_ME",
    templateSpreadsheetId: '1Y3mQriVBVmaIMfi6agkUHd3oKgedZIhMbQksZyOVZ-s',
    rootFolderId: '', // optional: folder where created copies should live
    signatureImageUrl: '', // optional: public image URL for =IMAGE() in A1
    logSpreadsheetId: '', // optional: spreadsheet to log submissions into
    logSheetName: 'Cashout Bot Log',
    routes: {
      cashout_submit: 'cashout_submit'
    }
  };

  const headerKey = 'X-API-Key';
  const body = JSON.parse((e && e.postData && e.postData.contents) || '{}');
  const requestKey = (e && e.parameter && e.parameter[headerKey]) || '';
  const providedKey = requestKey || body.apiKey || '';
  if (CONFIG.apiKey && providedKey !== CONFIG.apiKey) {
    return jsonOut({ ok: false, error: 'unauthorized' });
  }

  const route = body.route;
  const payload = body.payload || {};
  if (!CONFIG.routes[route]) {
    return jsonOut({ ok: false, error: 'unknown_route' });
  }

  if (route === 'cashout_submit') {
    return jsonOut(handleCashoutSubmit_(payload, CONFIG));
  }

  return jsonOut({ ok: false, error: 'unhandled_route' });
}

function handleCashoutSubmit_(payload, config) {
  const values = payload.values || {};
  const email = String(values.email || '').trim();
  if (!email) {
    return { ok: false, error: 'missing_email' };
  }

  const templateFile = DriveApp.getFileById(config.templateSpreadsheetId);
  const stamp = Utilities.formatDate(new Date(), Session.getScriptTimeZone(), 'yyyy-MM-dd HH:mm');
  const displayName = String(payload.display_name || payload.username || 'Member').trim();
  const cleanName = displayName.replace(/[\\/:*?"<>|#]/g, '').trim() || 'Member';
  const copyName = 'RS Cashout - ' + cleanName + ' - ' + stamp;

  const copyFile = config.rootFolderId
    ? templateFile.makeCopy(copyName, DriveApp.getFolderById(config.rootFolderId))
    : templateFile.makeCopy(copyName);

  const copySpreadsheet = SpreadsheetApp.openById(copyFile.getId());
  const sheet = copySpreadsheet.getSheets()[0];

  if (config.signatureImageUrl) {
    sheet.getRange('A1').setFormula('=IMAGE("' + config.signatureImageUrl + '")');
  }

  // Header row assumed to match the template screenshot.
  sheet.getRange('A3').setValue(values.name_of_shoe || '');
  sheet.getRange('B3').setValue(values.sku || '');
  sheet.getRange('C3').setValue(values.condition || '');
  sheet.getRange('D3').setValue(values.size || '');
  sheet.getRange('E3').setValue(values.qty || '');
  sheet.getRange('F3').setValue(values.price || '');
  sheet.getRange('G3').setValue(values.notes || '');

  // Store intake metadata off to the side so staff can see the owner email + Discord info.
  sheet.getRange('I1').setValue('Submission Email');
  sheet.getRange('J1').setValue(email);
  sheet.getRange('I2').setValue('Discord User');
  sheet.getRange('J2').setValue(String(payload.username || ''));
  sheet.getRange('I3').setValue('Discord User ID');
  sheet.getRange('J3').setValue(String(payload.user_id || ''));
  sheet.getRange('I4').setValue('Ticket Channel ID');
  sheet.getRange('J4').setValue(String(payload.channel_id || ''));
  sheet.getRange('I5').setValue('Created At');
  sheet.getRange('J5').setValue(String(payload.created_at || ''));

  // Sharing: requester + optional staff editors can edit; anyone with link can view.
  try {
    copyFile.addEditor(email);
  } catch (err) {
    // ignore invalid email
  }
  const extraEditors = payload.extra_editor_emails || payload.sheet_extra_editors || [];
  if (extraEditors && extraEditors.length) {
    for (let i = 0; i < extraEditors.length; i++) {
      const ex = String(extraEditors[i] || '').trim();
      if (!ex || ex.toLowerCase() === String(email).toLowerCase()) continue;
      try {
        copyFile.addEditor(ex);
      } catch (err2) {
        // ignore per-email failures
      }
    }
  }
  copyFile.setSharing(DriveApp.Access.ANYONE_WITH_LINK, DriveApp.Permission.VIEW);

  if (config.logSpreadsheetId) {
    appendLogRow_(config.logSpreadsheetId, config.logSheetName, payload, copySpreadsheet.getUrl(), copyFile.getId());
  }

  return {
    ok: true,
    sheet_url: copySpreadsheet.getUrl(),
    view_url: 'https://docs.google.com/spreadsheets/d/' + copyFile.getId() + '/edit?usp=drivesdk',
    sheet_name: copyName,
    file_id: copyFile.getId(),
    editor_email: email
  };
}

function appendLogRow_(spreadsheetId, sheetName, payload, sheetUrl, fileId) {
  const ss = SpreadsheetApp.openById(spreadsheetId);
  let sheet = ss.getSheetByName(sheetName);
  if (!sheet) {
    sheet = ss.insertSheet(sheetName);
    sheet.appendRow([
      'Timestamp', 'Guild ID', 'Channel ID', 'User ID', 'Username', 'Display Name',
      'Ticket Type', 'Ticket Label', 'Email', 'Sheet URL', 'File ID', 'Values JSON'
    ]);
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
    ((payload.values || {}).email) || '',
    sheetUrl || '',
    fileId || '',
    JSON.stringify(payload.values || {})
  ]);
}

function jsonOut(obj) {
  return ContentService
    .createTextOutput(JSON.stringify(obj))
    .setMimeType(ContentService.MimeType.JSON);
}
