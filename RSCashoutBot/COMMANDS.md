# RS Cashout Ticket Bot Commands

## Slash Commands

#### `/cashout`
- Description: Posts or refreshes the RS cashout panel in the configured panel channel.
- Admin Only: Yes
- Usage: `/cashout`

#### `/cashoutpanel`
- Description: Opens an ephemeral panel editor (edit title/description/footer, embed color, ticket banner URL), preview the panel embed, and post to the panel channel — similar to RSPromoBot’s builder flow.
- Admin Only: Yes
- Usage: `/cashoutpanel`
- Notes: Overrides are stored in `panel_overrides.json` on the bot host (merged with `messages.json` / `config.json` defaults).

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
