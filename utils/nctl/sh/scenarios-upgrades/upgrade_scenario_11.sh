#!/usr/bin/env bash

# ----------------------------------------------------------------
# Imports.
# ----------------------------------------------------------------

source "$NCTL/sh/utils/main.sh"
source "$NCTL/sh/node/svc_$NCTL_DAEMON_TYPE".sh
source "$NCTL/sh/scenarios/common/itst.sh"
source "$NCTL/sh/assets/upgrade.sh"


# ----------------------------------------------------------------
# MAIN
# ----------------------------------------------------------------

# Main entry point.
function _main()
{
    local STAGE_ID=${1}

    if [ ! -d $(get_path_to_stage "$STAGE_ID") ]; then
        log "ERROR :: stage $STAGE_ID has not been built - cannot run scenario"
        exit 1
    fi

    # Step 01: Start network from pre-built stage.
    _step_01 "$STAGE_ID"
    # Step 02: Await era-id >= 1.
    _step_02

    # TODO: Should add delegation to a genesis validator

    # TODO: Should wait 1 era

    # TODO: Should undelegate before upgrade

    # Join rest of nodes
    _step_03
    # Set initial protocol version for use later.
    INITIAL_PROTOCOL_VERSION=$(get_node_protocol_version 1)
    # Step 04: Await era-id += 1.
    _step_04
    # Generate global state update and stage upgrade
    _step_05 "$STAGE_ID"
    # Step 07: awaiting 2 eras + 1 block.
    _step_06
    # Step 06: Assert chain is progressing at all nodes.
    _step_07 "$INITIAL_PROTOCOL_VERSION"

    # TODO: Should wait unbonding delay

    # TODO: Should check balances for the delegator

    # Health checks
    _step_16
    # Finish up
    _step_17

}

# Step 01: Start network from pre-built stage.
function _step_01()
{
    local STAGE_ID=${1}
    local PATH_TO_STAGE
    local PATH_TO_PROTO1

    PATH_TO_STAGE=$(get_path_to_stage "$STAGE_ID")
    pushd "$PATH_TO_STAGE"
    PATH_TO_PROTO1=$(ls -d */ | sort | head -n 1 | tr -d '/')
    popd

    log_step_upgrades 1 "starting network from stage ($STAGE_ID)"

    source "$NCTL/sh/assets/setup_from_stage.sh" stage="$STAGE_ID"
    source "$NCTL/sh/node/start.sh" node=all
}

# Step 02: Await era-id >= 1.
function _step_02()
{
    log_step_upgrades 2 "awaiting genesis era completion"

    do_await_genesis_era_to_complete 'false'
}

function _step_03()
{
    log_step_upgrades 3 "Joining other nodes"

    local TRUSTED_HASH=$(do_read_lfb_hash '1')
    for NODE_ID in $(seq 6 10); do
        do_node_start "$NODE_ID" "$TRUSTED_HASH"
    done
}

# Step 04: Await era-id += 1.
function _step_04()
{
    log_step_upgrades 4 "awaiting next era"

    nctl-await-n-eras offset='1' sleep_interval='2.0' timeout='180'
}

function _step_05() {
    local STAGE_ID=${1}
    local PATH_TO_NET=$(get_path_to_net)

    log_step "stopping the network for an emergency upgrade"
    ACTIVATE_ERA="$(get_chain_era)"
    log "emergency upgrade activation era = $ACTIVATE_ERA"
    local ERA_ID=$((ACTIVATE_ERA - 1))
    local BLOCK=$(get_switch_block "1" "32" "" "$ERA_ID")
    # read the latest global state hash
    STATE_HASH=$(echo "$BLOCK" | jq -r '.header.state_root_hash')
    # save the LFB hash to use as the trusted hash for the restart
    TRUSTED_HASH=$(echo "$BLOCK" | jq -r '.hash')
    log "state hash = $STATE_HASH"
    log "trusted hash = $TRUSTED_HASH"
    # stop the network
    #do_node_stop_all
    for NODE_ID in $(seq 1 10); do
        local PATH_TO_NODE=$(get_path_to_node $NODE_ID)

        do_node_stop "$NODE_ID"
        _generate_global_state_update "1_5_8" "$STATE_HASH" 1 "$(get_count_of_genesis_nodes)"
        do_node_start "$NODE_ID" "$TRUSTED_HASH"
        sleep 1

        source "$NCTL/sh/assets/upgrade_from_stage_single_node.sh" \
            stage="$STAGE_ID" \
            verbose=true \
            node="$NODE_ID" \
            era=3 \
            hard_reset="True"
        cp "$PATH_TO_NET"/chainspec/"1_5_8"/global_state.toml \
            "$PATH_TO_NODE"/config/"1_5_8"/global_state.toml
        
        # remove stored state of the launcher - this will make the launcher start from the highest
        # available version instead of from the previously executed one
        if [ -e "$PATH_TO_NODE"/config/casper-node-launcher-state.toml ]; then
            rm "$PATH_TO_NODE"/config/casper-node-launcher-state.toml
        fi
    done
}

# Step 07: awaiting 2 eras + 1 block.
function _step_06()
{
    log_step_upgrades 7 "... awaiting 2 eras + 1 block"

    nctl-await-n-eras offset='2' sleep_interval='5.0' timeout='180'
    await_n_blocks 1
}

# Step 06: Assert chain is progressing at all nodes.
function _step_07()
{
    local N1_PROTOCOL_VERSION_INITIAL=${1}
    local TIMEOUT=${2:-'60'}
    local HEIGHT_1
    local HEIGHT_2
    local NODE_ID

    log_step_upgrades 6 "asserting node upgrades"

    while [ "$TIMEOUT" -ge 0 ]; do
        # Assert no nodes have stopped.
        if [ "$(get_count_of_up_nodes)" != 10 ]; then
            if [ "$TIMEOUT" != '0' ]; then
                log "...waiting for nodes to come up, timeout=$TIMEOUT"
                sleep 1
                TIMEOUT=$((TIMEOUT-1))
            else
                log "ERROR :: protocol upgrade failure - >= 1 nodes have stopped"
                exit 1
            fi
        else
            log "... all nodes up! [continuing]"
            break
        fi
    done

    # Assert no nodes have stalled.
    HEIGHT_1=$(get_chain_height)
    await_n_blocks 2
    for NODE_ID in $(seq 1 10)
    do
        HEIGHT_2=$(get_chain_height "$NODE_ID")
        if [ "$HEIGHT_2" != "N/A" ] && [ "$HEIGHT_2" -le "$HEIGHT_1" ]; then
            log "ERROR :: protocol upgrade failure - node $NODE_ID has stalled - current height $HEIGHT_2 is <= starting height $HEIGHT_1"
            exit 1
        fi
    done

    # Assert node-1 protocol version incremented.
    N1_PROTOCOL_VERSION=$(get_node_protocol_version 1)
    if [ "$N1_PROTOCOL_VERSION" == "$N1_PROTOCOL_VERSION_INITIAL" ]; then
        log "ERROR :: protocol upgrade failure - >= protocol version did not increment"
        exit 1
    else
        log "Node 1 upgraded successfully: $N1_PROTOCOL_VERSION_INITIAL -> $N1_PROTOCOL_VERSION"
    fi

    # Assert nodes are running same protocol version.
    for NODE_ID in $(seq 2 10)
    do
        NX_PROTOCOL_VERSION=$(get_node_protocol_version "$NODE_ID")
        if [ "$NX_PROTOCOL_VERSION" != "$N1_PROTOCOL_VERSION" ]; then
            log "ERROR :: protocol upgrade failure - >= nodes are not all running same protocol version"
            exit 1
        else
            log "Node $NODE_ID upgraded successfully: $N1_PROTOCOL_VERSION_INITIAL -> $NX_PROTOCOL_VERSION"
        fi
    done
}

# Step 12: Undelegate previous user
function _step_12()
{
    local NODE_ID=${1:-'5'}
    local ACCOUNT_ID=${2:-'7'}
    local AMOUNT=${3:-'500000000000'}

    log_step_upgrades 12 "Undelegating $AMOUNT to account-$ACCOUNT_ID from validator-$NODE_ID"

    source "$NCTL/sh/contracts-auction/do_delegate_withdraw.sh" \
            amount="$AMOUNT" \
            delegator="$ACCOUNT_ID" \
            validator="$NODE_ID"
}

# Step 13: Await 4 eras
function _step_13()
{
    log_step_upgrades 13 "Awaiting Auction_Delay = 1 + 1"
    nctl-await-n-eras offset='2' sleep_interval='5.0' timeout='300'
}

# Step 07: Assert USER_ID is NOT a delegatee
function _step_15()
{
    local USER_ID=${1:-'7'}
    local USER_PATH
    local HEX
    local AUCTION_INFO_FOR_HEX

    USER_PATH=$(get_path_to_user "$USER_ID")
    HEX=$(cat "$USER_PATH"/public_key_hex | tr '[:upper:]' '[:lower:]')
    AUCTION_INFO_FOR_HEX=$(nctl-view-chain-auction-info | jq --arg node_hex "$HEX" '.auction_state.bids[]| select(.bid.delegators[].public_key | ascii_downcase == $node_hex)')

    log_step_upgrades 15 "Asserting user-$USER_ID is NOT a delegatee"

    if [ ! -z "$AUCTION_INFO_FOR_HEX" ]; then
        log "ERROR: user-$USER_ID found in auction info delegators!"
        log "... public_key_hex: $HEX"
        echo "$AUCTION_INFO_FOR_HEX"
        exit 1
    else
        log "... Could not find $HEX in auction info delegators! [expected]"
    fi
}

# Step 16: Run NCTL health checks
function _step_16()
{
    # restarts=6 - Nodes that upgrade
    log_step_upgrades 16 "running health checks"
    source "$NCTL"/sh/scenarios/common/health_checks.sh \
            errors='0' \
            equivocators='0' \
            doppels='0' \
            crashes=0 \
            restarts=6 \
            ejections=0
}

# Step 17: Terminate.
function _step_17()
{
    log_step_upgrades 17 "test successful - tidying up"

    source "$NCTL/sh/assets/teardown.sh"

    log_break
}

# ----------------------------------------------------------------
# ENTRY POINT
# ----------------------------------------------------------------

unset _STAGE_ID
unset INITIAL_PROTOCOL_VERSION
unset PROTOCOL_VERSION

for ARGUMENT in "$@"
do
    KEY=$(echo "$ARGUMENT" | cut -f1 -d=)
    VALUE=$(echo "$ARGUMENT" | cut -f2 -d=)
    case "$KEY" in
        stage) _STAGE_ID=${VALUE} ;;
        *)
    esac
done

_main "${_STAGE_ID:-1}"
