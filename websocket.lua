local server = require "resty.websocket.server"
local cjson = require("cjson.safe").new()
local redis = require('resty.redis')

local timeout = 5000
local recv_threshold = 5 -- socket will be closed by server while recv_frame's continuous failure times exceeding this threshold, and a test ping is not answered by pong.
local recv_errors = 0
local test_ping_sent = false
local test_ping_sendings = 0
local test_ping_threshold = 5 -- socket will be closed while ping_frame sended times exceeding this threshold.

local redis_host = "127.0.0.1"
local redis_port = 6379

local socket, err = server:new {
    timeout = timeout, -- in milliseconds
    max_payload_len = 65535,
}

if not socket then
    ngx.log(ngx.ERR, "failed to new websocket: ", err)
    return ngx.exit(444)
end

socket:set_timeout(timeout)
ngx.log(ngx.DEBUG, "websocket established with timeout: ", timeout, ". remote_addr: ", ngx.var.remote_addr)

local subscribe_co = nil
local should_subscribe_stop = false

function subscribe(channels)
    local cli = redis:new()
    cli:set_timeout(timeout)

    local ok, err = cli:connect(redis_host, redis_port)
    if not ok then
        ngx.say("failed to connect redis: ", err)
        return ngx.exit(444)
    end
    ngx.log(ngx.DEBUG, "redis connected with timeout: ", timeout)

    local resp, err = cli:subscribe(unpack(channels))
    if not resp then
        ngx.log(ngx.ERR, "failed to subscribe channels ", cjson.encode(channels), ". : ", err)
        return ngx.exit(444)
    end

    ngx.log(ngx.INFO, "channels subscribed: ", cjson.encode(resp))

    while true do
        if should_subscribe_stop then
            break
        end

        local resp, err = cli:read_reply()
        if not resp then
            ngx.log(ngx.DEBUG, "no redis reply available. remote_addr: ", ngx.var.remote_addr)
            goto continue
        end

        ngx.log(ngx.DEBUG, "redis replied: ", cjson.encode(resp))
        if resp[1] == 'message' then
            local channel = resp[2]
            local msg = resp[3]

            local ws_resp, ws_err = socket:send_text(cjson.encode({ message = msg, channel = channel }))
            if not ws_resp then
                ngx.log(ngx.ERR, "failed to send a text frame: ", ws_err)
            end
        end

        :: continue ::
    end

    local resp, err = cli:unsubscribe(unpack(channels))
    if not resp then
        ngx.log(ngx.ERR, "failed to unsubscribe channels ", cjson.encode(channels), ". : ", err)
        cli.close()
        return
    end

    local ok, err = cli:set_keepalive(timeout * 100, 10000)
    if not ok then
        ngx.log(ngx.ERR, "failed to close redis connection: ", err)
    end

    ngx.log(ngx.DEBUG, "redis connection closed. remote_addr: ", ngx.var.remote_addr)
end

while true do
    local data, type, err = socket:recv_frame()

    if not data then
        if test_ping_sent and recv_errors > recv_threshold then
            ngx.log(ngx.INFO, "recv_threshold exceeded: ", recv_errors, ", and no pong received.")
            break
        end

        if not subscribe_co and test_ping_sendings > test_ping_threshold then
            ngx.log(ngx.INFO, "test_ping_threshold exceeded. remote_addr: ", ngx.var.remote_addr, ", and not subscribing.")
            break
        end

        recv_errors = recv_errors + 1
        if recv_errors > recv_threshold then
            socket:send_ping("ping")
            test_ping_sendings = test_ping_sendings + 1
            test_ping_sent = true
        end

        goto continue
    end

    recv_errors = 0

    if type == "close" then
        break
    end

    if type == "ping" then
        -- send a pong frame back:
        local bytes, err = socket:send_pong(data)
        if not bytes then
            ngx.log(ngx.ERR, "failed to send a pong frame: ", err)
            break
        end
    elseif type == "pong" then
        ngx.log(ngx.DEBUG, "a pong frame received. remote_addr: ", ngx.var.remote_addr)
        test_ping_sent = false
    elseif type == "text" then
        local value, err = cjson.decode(data);
        if not value then
            ngx.log(ngx.ERR, "failed to decode json: ", data, " err: ", err)
            goto continue
        end

        local channels = value["channels"]
        if not channels or #channels == 0 then
            if not subscribe_co then
                local resp, err = socket:send_text(cjson.encode({ code = "1", msg = "empty channels is not allowed." }))
                if not resp then
                    ngx.log(ngx.ERR, "failed to send a text frame: ", err)
                end
            end

            goto continue
        end

        if subscribe_co then
            should_subscribe_stop = true
            ngx.thread.wait(subscribe_co)
            should_subscribe_stop = false
        end

        local err = nil
        subscribe_co, err = ngx.thread.spawn(subscribe, channels)
        if not subscribe_co then
            ngx.log(ngx.ERR, "failed to spawn a redis subscribe thread: ", err)
            break
        end
    else
        ngx.log(ngx.INFO, "received a frame of type ", type, " and payload ", data)
    end

    :: continue ::
end

if subscribe_co then
    should_subscribe_stop = true
    ngx.thread.wait(subscribe_co)
end

local bytes, err = socket:send_close()
if not bytes then
    ngx.log(ngx.ERR, "failed to send the close frame: ", err)
    return ngx.exit(444)
end

ngx.log(ngx.DEBUG, "websocket closed. remote_addr: ", ngx.var.remote_addr)
