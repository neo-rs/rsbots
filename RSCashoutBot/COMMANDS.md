# RS Cashout Ticket Bot Commands

## Slash Commands

#### `/cashout`
- Description: Posts or refreshes the RS cashout panel in the configured panel channel.
- Admin Only: Yes
- Usage: `/cashout`

#### `/cashoutpanel`
- Description: Opens an ephemeral **Cashout message editor**: edit the public panel card, the **Submit Cashout** ticket welcome embed (title, body, “Next step” template with `{link}` / `{view}`), the **member DM** copy and field labels, extra **Google Sheet editor** emails (merged with `config.json` → `google_sheet.extra_editor_emails`), plus previews for panel / ticket / DM and post panel.
- Admin Only: Yes
- Usage: `/cashoutpanel`
- Notes: Runtime overrides live in `panel_overrides.json`. **Apps Script** must include the `extra_editor_emails` handler (see `google_apps_script.js` in this folder). Redeploy the web app after changing the script.

#### `/cashoutnew`
- Description: Opens a new Request/Submit flow for the member.
- Admin Only: No
- Usage: `/cashoutnew`
- Notes: Good for repeat sellers who need another cashout sheet copy without hunting for the panel message.

#### `/ticketclose`
- Description: Closes the current ticket channel after the configured delay.
- Admin Only: No
- Usage: `/ticketclose`

#### `/ticketadd`
- Description: Adds a member to the current ticket channel.
- Admin Only: Yes
- Usage: `/ticketadd member:@user`

#### `/ticketremove`
- Description: Removes a member from the current ticket channel.
- Admin Only: Yes
- Usage: `/ticketremove member:@user`

## Summary
- Total Commands: 6
- Admin Commands: 4
- Public Commands: 2
