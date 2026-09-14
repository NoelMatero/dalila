use crate::state::MemberTable;

pub async fn start_udp_detector(table: MemberTable) -> anyhow::Result<()> {
    // TODO: this needs a tokio::time::interval — a bare `loop` here spins a core.
    // One probe per protocol period: walk `table.shuffled_ids()`, and ask for a
    // fresh shuffle when the pass runs out.
    let _order = table.shuffled_ids();

    todo!()

    /*
            start pinging nodes randomly with eihter connect or bind,
            update incarnation, suspicion and the member list etc.
            so we first sort the list of memebr ndoes and then ping them one by one
            once we come to the end of the list, we re-sort the list

            1. get the list, by this time we've joined everything we wanted to in the cli args.
            2. start an async loop
            3. get a node from the rng list
            4. ping that node
            5. go to the next node and repeat
            6. ...
            7. once we're done with that last, we just reshuffle the list and restart
    */
}
