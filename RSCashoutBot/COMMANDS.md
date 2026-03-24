# RS Cashout Ticket Bot Commands

## Slash Commands

#### `/ticketpanel`
- Description: Posts the RS cashout ticket panel in the configured panel channel.
- Admin Only: Yes
- Usage: `/ticketpanel`
- Notes: The command refuses to run outside the configured panel channel.

#### `/ticketclose`
- Description: Closes the current ticket channel after the configured delay.
- Admin Only: No
- Usage: `/ticketclose`
- Notes: Also available through the Close Ticket button inside each ticket.

#### `/ticketadd`
- Description: Adds a member to the current ticket channel.
- Admin Only: Yes
- Usage: `/ticketadd member:@user`

#### `/ticketremove`
- Description: Removes a member from the current ticket channel.
- Admin Only: Yes
- Usage: `/ticketremove member:@user`

## Summary
- Total Commands: 4
- Admin Commands: 3
- Public Commands: 1
