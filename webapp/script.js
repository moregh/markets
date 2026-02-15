// script.js
// If served from http(s), default API host to same hostname on port 8000.
// If opened as file://, fall back to localhost.
const API_BASE = (window.location.protocol === 'http:' || window.location.protocol === 'https:')
    ? `${window.location.protocol}//${window.location.hostname}:8000`
    : 'http://localhost:8000';
let authToken = null;
let refreshToken = null;
let refreshInFlight = null;
let tokenRefreshTimer = null;
const TOKEN_REFRESH_LEEWAY_SECONDS = 60;
let currentUser = null;
let selectedMarket = null;
let selectedMarketId = null;
let refreshInterval = null;
let statsInterval = null;
let myOrdersInterval = null;
let expiryTicker = null;
let marketDataInFlight = null;
let systemStatsInFlight = null;
let marketWalletInFlight = null;
let marketWalletLastFetchMs = 0;
let myOrdersInFlight = null;
let myOrdersLastFetchMs = 0;

// UI polling cadence.
const MARKET_REFRESH_MS = 500;
const SYSTEM_STATS_REFRESH_MS = 500;
const EXPIRY_TICK_MS = 500;
const MARKET_WALLET_REFRESH_MS = 500;
const MY_ORDERS_REFRESH_MS = 500;
let marketsById = new Map();
let walletSnapshot = null;
let walletDetailsSnapshot = null;
let ordersSnapshot = [];
let orderMatchesCache = new Map();
let adminUsersSnapshot = [];
let adminSelectedUserId = null;
let adminSelectedUserAccount = null;
let recentTradesState = {
    marketId: null,
    keys: [],
    initialized: false
};
let lastRecentTradesByMarket = new Map();
let marketPriceChartState = {
    marketId: null,
    points: [],
    seenKeys: new Set(),
    initialized: false
};
let marketPriceChartResizeObserver = null;
let marketPulseState = {
    marketId: null,
    points: [],
    initialized: false
};
let marketPulseChartResizeObserver = null;
let marketVolumeChartResizeObserver = null;
let marketValueChartResizeObserver = null;
let marketExecutionSeriesState = {
    marketId: null,
    points: [],
    seenKeys: new Set(),
    initialized: false
};
let tradePanelState = {
    open: false,
    mode: 'limit', // limit|fill
    sourceSide: null
};
let orderbookState = {
    marketId: null,
    buys: [],
    sells: [],
    initialized: false
};
let marketSummary = {
    activeBuys: 0,
    activeSells: 0,
    totalBuyQty: 0,
    totalSellQty: 0,
    totalBuyValue: 0,
    totalSellValue: 0
};
let latestOrderbook = { buys: [], sells: [] };
const integerFormatter = new Intl.NumberFormat('en-US');
const priceFormatter = new Intl.NumberFormat('en-US', {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2
});
const compactFormatter = new Intl.NumberFormat('en-US', {
    notation: 'compact',
    maximumFractionDigits: 1
});
const ACTIVE_ORDER_STATES = ['PENDING_SUBMIT', 'OPEN', 'PARTIAL'];

// Initialize
document.addEventListener('DOMContentLoaded', () => {
    ensureMarketPriceChartWiring();
    ensureMarketPulseChartWiring();
    ensureMarketVolumeChartWiring();
    ensureMarketValueChartWiring();
    bindTradePanelControls();
    closeTradePanel();
    checkAuth();
});

function decodeJwtPayload(token) {
    if (!token || typeof token !== 'string') return null;
    const parts = token.split('.');
    if (parts.length !== 3) return null;
    const base64Url = parts[1];
    const base64 = base64Url.replace(/-/g, '+').replace(/_/g, '/');
    const padded = base64 + '='.repeat((4 - (base64.length % 4)) % 4);
    try {
        const json = atob(padded);
        return JSON.parse(json);
    } catch {
        return null;
    }
}

function getAccessTokenExpSeconds(token) {
    const payload = decodeJwtPayload(token);
    const exp = payload?.exp;
    return Number.isFinite(exp) ? Number(exp) : null;
}

function scheduleProactiveRefresh() {
    if (tokenRefreshTimer) {
        clearTimeout(tokenRefreshTimer);
        tokenRefreshTimer = null;
    }
    if (!authToken || !refreshToken) {
        return;
    }
    const exp = getAccessTokenExpSeconds(authToken);
    if (!exp) {
        return;
    }
    const now = Date.now() / 1000;
    const refreshIn = Math.max(5, Math.floor(exp - now - TOKEN_REFRESH_LEEWAY_SECONDS));
    tokenRefreshTimer = setTimeout(async () => {
        await refreshAccessToken(); // errors handled inside; no forced logout on transient failures
    }, refreshIn * 1000);
}

function setSessionTokens(access, refresh) {
    authToken = access;
    refreshToken = refresh;
    localStorage.setItem('authToken', authToken);
    localStorage.setItem('refreshToken', refreshToken);
    scheduleProactiveRefresh();
}

function clearSessionTokens() {
    authToken = null;
    refreshToken = null;
    refreshInFlight = null;
    localStorage.removeItem('authToken');
    localStorage.removeItem('refreshToken');
    if (tokenRefreshTimer) {
        clearTimeout(tokenRefreshTimer);
        tokenRefreshTimer = null;
    }
}

// Authentication
function checkAuth() {
    authToken = localStorage.getItem('authToken');
    refreshToken = localStorage.getItem('refreshToken');

    if (authToken) {
        scheduleProactiveRefresh();
        initializeApp();
    } else {
        showLogin();
    }
}

function showLogin() {
    document.getElementById('loginScreen').style.display = 'flex';
    document.getElementById('mainApp').style.display = 'none';
}

function showApp() {
    document.getElementById('loginScreen').style.display = 'none';
    document.getElementById('mainApp').style.display = 'flex';
}

function switchLoginTab(tab, tabButton) {
    document.querySelectorAll('.login-tab').forEach(t => t.classList.remove('active'));
    document.querySelectorAll('.login-form').forEach(f => f.classList.remove('active'));

    const activeButton = tabButton || window.event?.target;
    if (activeButton) {
        activeButton.classList.add('active');
    }
    document.getElementById(tab + 'Form').classList.add('active');
}

async function login() {
    const username = document.getElementById('loginUsername').value;
    const password = document.getElementById('loginPassword').value;

    if (!username || !password) {
        showError('loginError', 'Please enter username and password');
        return;
    }

    try {
        const response = await fetch(`${API_BASE}/auth/login`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ username, password })
        });

        const data = await response.json();

        if (response.ok) {
            setSessionTokens(data.access_token, data.refresh_token);
            initializeApp();
        } else {
            showError('loginError', data.detail || 'Login failed');
        }
    } catch (error) {
        showError('loginError', 'Connection error');
    }
}

async function register() {
    const username = document.getElementById('registerUsername').value;
    const email = document.getElementById('registerEmail').value;
    const password = document.getElementById('registerPassword').value;

    if (!username || !email || !password) {
        showError('registerError', 'Please fill all fields');
        return;
    }

    try {
        const response = await fetch(`${API_BASE}/auth/register`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ username, email, password })
        });

        const data = await response.json();

        if (response.ok) {
            setSessionTokens(data.access_token, data.refresh_token);
            initializeApp();
        } else {
            showError('registerError', data.detail || 'Registration failed');
        }
    } catch (error) {
        showError('registerError', 'Connection error');
    }
}

function logout() {
    if (refreshInterval) {
        clearInterval(refreshInterval);
        refreshInterval = null;
    }
    if (statsInterval) {
        clearInterval(statsInterval);
        statsInterval = null;
    }
    if (myOrdersInterval) {
        clearInterval(myOrdersInterval);
        myOrdersInterval = null;
    }
    if (expiryTicker) {
        clearInterval(expiryTicker);
        expiryTicker = null;
    }

    authToken = null;
    refreshToken = null;
    currentUser = null;
    selectedMarket = null;
    selectedMarketId = null;
    walletSnapshot = null;
    walletDetailsSnapshot = null;
    adminUsersSnapshot = [];
    adminSelectedUserId = null;
    adminSelectedUserAccount = null;
    myOrdersInFlight = null;
    myOrdersLastFetchMs = 0;
    clearSessionTokens();
    renderCurrentMarketWalletHoldings();
    showLogin();
}

async function initializeApp() {
    try {
        const response = await apiRequest('/auth/me');
        currentUser = response;

        document.getElementById('currentUsername').textContent = currentUser.username;

        if (currentUser.is_admin) {
            document.getElementById('adminTab').style.display = 'inline-flex';
        } else {
            document.getElementById('adminTab').style.display = 'none';
        }

        showApp();
        setupTabNavigation();
        updateSidebarContext('markets');
        loadSystemStats();
        loadMarkets();
        loadMyWallet();

        if (statsInterval) {
            clearInterval(statsInterval);
        }
        statsInterval = setInterval(loadSystemStats, SYSTEM_STATS_REFRESH_MS);
        if (myOrdersInterval) {
            clearInterval(myOrdersInterval);
        }
        myOrdersInterval = setInterval(() => {
            refreshMyOrdersSnapshot().catch(() => {});
        }, MY_ORDERS_REFRESH_MS);

    } catch (error) {
        logout();
    }
}

function isWalletTabActive() {
    const walletPanel = document.getElementById('wallet');
    return !!walletPanel && walletPanel.classList.contains('active');
}

async function refreshAccessToken() {
    if (!refreshToken) {
        return { ok: false, fatal: true };
    }

    if (refreshInFlight) {
        return refreshInFlight;
    }

    try {
        refreshInFlight = (async () => {
            const response = await fetch(`${API_BASE}/auth/refresh`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ refresh_token: refreshToken })
            });

            let data = {};
            try {
                data = await response.json();
            } catch {
                data = {};
            }

            if (response.ok) {
                setSessionTokens(data.access_token, data.refresh_token);
                return { ok: true, fatal: false };
            }

            // Invalid/revoked refresh token: fatal; user must log in again.
            if (response.status === 400 || response.status === 401) {
                return { ok: false, fatal: true, detail: data.detail };
            }

            // Transient server failure (DB contention etc): do not log out.
            return { ok: false, fatal: false, detail: data.detail };
        })();
        return await refreshInFlight;
    } catch (error) {
        return { ok: false, fatal: false, detail: 'Refresh failed' };
    } finally {
        refreshInFlight = null;
    }
}

async function apiRequest(endpoint, options = {}) {
    // Proactive refresh: if token is near expiry, refresh before sending.
    if (authToken && refreshToken) {
        const exp = getAccessTokenExpSeconds(authToken);
        if (exp) {
            const now = Date.now() / 1000;
            if ((exp - now) <= TOKEN_REFRESH_LEEWAY_SECONDS) {
                const res = await refreshAccessToken();
                if (res?.fatal) {
                    logout();
                    throw new Error('Session expired');
                }
            }
        }
    }

    const headers = {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${authToken}`,
        ...options.headers
    };

    const response = await fetch(`${API_BASE}${endpoint}`, {
        ...options,
        headers
    });

    if (response.status === 401 && !options._retry) {
        const refreshed = await refreshAccessToken();
        if (refreshed?.ok) {
            return apiRequest(endpoint, { ...options, _retry: true });
        }
        if (refreshed?.fatal) {
            logout();
            throw new Error('Session expired');
        }
        // Transient refresh failure; keep session and surface error.
        throw new Error('Auth refresh temporarily unavailable');
    }

    if (response.status === 401) {
        logout();
        throw new Error('Unauthorized');
    }

    let data = {};
    try {
        data = await response.json();
    } catch {
        data = {};
    }

    if (!response.ok) {
        throw new Error(data.detail || 'Request failed');
    }

    return data;
}

// Tab Navigation
function setupTabNavigation() {
    const tabs = document.querySelectorAll('.main-tab');
    tabs.forEach(tab => {
        if (tab.dataset.bound === 'true') {
            return;
        }
        tab.addEventListener('click', () => {
            const targetTab = tab.dataset.tab;

            tabs.forEach(t => t.classList.remove('active'));
            tab.classList.add('active');

            document.querySelectorAll('.tab-panel').forEach(panel => {
                panel.classList.remove('active');
            });
            document.getElementById(targetTab).classList.add('active');

            if (targetTab === 'wallet') {
                loadMyWallet();
            } else if (targetTab === 'markets') {
                loadMarkets();
                if (selectedMarketId) {
                    loadMarketData();
                }
            } else if (targetTab === 'admin') {
                if (currentUser && currentUser.is_admin) {
                    adminGetFeesCollected();
                    adminLoadFeeConfig();
                    adminLoadUsers();
                    adminLoadPerf();
                }
            }

            updateSidebarContext(targetTab);
        });
        tab.dataset.bound = 'true';
    });
}

function updateSidebarContext(tabName) {
    const marketWrap = document.getElementById('sidebarMarketListWrap');
    marketWrap.style.display = tabName === 'markets' ? 'flex' : 'none';
}

// System Stats
async function loadSystemStats() {
    if (systemStatsInFlight) {
        return systemStatsInFlight;
    }
    try {
        systemStatsInFlight = apiRequest('/stats');
        const data = await systemStatsInFlight;

        document.getElementById('activeOrders').textContent = formatInteger(data.orders.total_active);
        document.getElementById('activeBuyOrders').textContent = formatInteger(data.orders.active_buy_orders);
        document.getElementById('activeSellOrders').textContent = formatInteger(data.orders.active_sell_orders);
        document.getElementById('matchesLastHour').textContent = formatInteger(data.matching.last_hour_count);
    } catch (error) {
        console.error('Error loading stats:', error);
    } finally {
        systemStatsInFlight = null;
    }
}

// Markets
async function loadMarkets() {
    try {
        const markets = await apiRequest('/markets');
        marketsById = new Map(markets.map(market => [market.id, market.ticker]));
        const list = document.getElementById('sidebarMarketList');
        list.innerHTML = markets.length > 0
            ? markets.map(m => `
                <button
                    type="button"
                    class="sidebar-market-item ${Number(selectedMarketId) === Number(m.id) ? 'active' : ''}"
                    data-market-id="${m.id}"
                    data-market-ticker="${m.ticker}"
                >${m.ticker}</button>
            `).join('')
            : '<div class="sidebar-market-empty">No markets available</div>';

        list.querySelectorAll('.sidebar-market-item').forEach((btn) => {
            btn.addEventListener('click', () => {
                selectMarket(btn.dataset.marketTicker, Number(btn.dataset.marketId));
            });
        });

        if (markets.length > 0 && !selectedMarket) {
            selectMarket(markets[0].ticker, markets[0].id);
        }
    } catch (error) {
        console.error('Error loading markets:', error);
    }
}

async function selectMarket(ticker, marketId) {
    if (!ticker || !marketId) return;

    selectedMarket = ticker;
    selectedMarketId = marketId;
    document.getElementById('currentMarket').textContent = ticker;
    renderCurrentMarketWalletHoldings();
    document.querySelectorAll('.sidebar-market-item').forEach((btn) => {
        const selected = Number(btn.dataset.marketId) === Number(marketId);
        btn.classList.toggle('active', selected);
    });

    document.getElementById('noMarketSelected').style.display = 'none';
    document.getElementById('marketContent').style.display = 'flex';

    // Reset incremental render state on market switch.
    orderbookState = { marketId: marketId, buys: [], sells: [], initialized: false };
    recentTradesState = { marketId: marketId, keys: [], initialized: false };
    resetMarketPriceChart(marketId);
    resetMarketPulseChart(marketId);
    resetMarketExecutionCharts(marketId);
    closeTradePanel();

    // Refresh wallet immediately for the market header.
    refreshMarketWalletHoldings({ force: true });

    await loadMarketData();

    if (refreshInterval) clearInterval(refreshInterval);
    refreshInterval = setInterval(loadMarketData, MARKET_REFRESH_MS);
}

async function loadMarketData() {
    if (!selectedMarketId) return;

    if (marketDataInFlight) {
        return marketDataInFlight;
    }

    marketDataInFlight = Promise.all([
        loadOrderBook(),
        loadRecentTrades(),
        loadMarketStats(),
        refreshMarketWalletHoldings(),
        refreshMyOrdersSnapshot().catch(() => ordersSnapshot)
    ]).finally(() => {
        marketDataInFlight = null;
    });

    return marketDataInFlight;
}

async function loadOrderBook() {
    try {
        const data = await apiRequest(`/markets/${selectedMarketId}/orderbook`);
        const apiBuys = Array.isArray(data?.buys) ? data.buys : [];
        const apiSells = Array.isArray(data?.sells) ? data.sells : [];
        const displayBuys = mergeOwnOrdersIntoOrderbook(apiBuys, 'buy');
        const displaySells = mergeOwnOrdersIntoOrderbook(apiSells, 'sell');
        latestOrderbook = {
            buys: apiBuys,
            sells: apiSells
        };

        renderOrderbookSide('sellOrders', displaySells, 'sell');
        renderOrderbookSide('buyOrders', displayBuys, 'buy');

        if (apiBuys.length > 0 && apiSells.length > 0) {
            const rawSpread = Number(apiSells[0].price) - Number(apiBuys[0].price);
            const spread = Number.isFinite(rawSpread) ? Math.max(0.01, rawSpread) : 0.01;
            document.getElementById('spreadValue').textContent = formatPrice(spread);
            document.getElementById('bestBidValue').textContent = formatPrice(apiBuys[0].price);
            document.getElementById('bestAskValue').textContent = formatPrice(apiSells[0].price);
            document.getElementById('midPriceValue').textContent = formatPrice((apiBuys[0].price + apiSells[0].price) / 2);
        } else {
            document.getElementById('spreadValue').textContent = '-';
            document.getElementById('bestBidValue').textContent = '-';
            document.getElementById('bestAskValue').textContent = '-';
            document.getElementById('midPriceValue').textContent = '-';
        }

        marketSummary.totalBuyQty = apiBuys.reduce((sum, level) => sum + Number(level.quantity || 0), 0);
        marketSummary.totalSellQty = apiSells.reduce((sum, level) => sum + Number(level.quantity || 0), 0);
        marketSummary.totalBuyValue = apiBuys.reduce((sum, level) => {
            const price = Number(level.price || 0);
            const qty = Number(level.quantity || 0);
            return sum + (Number.isFinite(price) && Number.isFinite(qty) ? (price * qty) : 0);
        }, 0);
        marketSummary.totalSellValue = apiSells.reduce((sum, level) => {
            const price = Number(level.price || 0);
            const qty = Number(level.quantity || 0);
            return sum + (Number.isFinite(price) && Number.isFinite(qty) ? (price * qty) : 0);
        }, 0);

        renderOrderbookDeltaBar({ buys: apiBuys, sells: apiSells });
        renderMarketSummary();
        ingestMarketPulseSnapshot({ buys: apiBuys, sells: apiSells });
        renderMarketPulseChart();
        syncTradePanelCapacityHint();
        bindInlineOrderActionButtons();
    } catch (error) {
        console.error('Error loading order book:', error);
    }
}

function orderLevelKeyBase(level) {
    const orderId = Number(level?.order_id);
    if (Number.isFinite(orderId) && orderId > 0) {
        return `oid:${orderId}`;
    }
    const price = Number(level.price || 0).toFixed(2);
    const mine = level.is_mine ? '1' : '0';
    const cancel = level.cancel_token ? String(level.cancel_token) : '';
    // IMPORTANT: exclude quantity/value/expiry so existing rows update in-place on every tick.
    return `${price}|${mine}|${cancel}`;
}

function listKeysWithOccurrence(levels) {
    if (Array.isArray(levels) && levels.every(lvl => Number.isFinite(Number(lvl?.order_id)) && Number(lvl?.order_id) > 0)) {
        return levels.map(lvl => `oid:${Number(lvl.order_id)}`);
    }
    const counts = new Map();
    return levels.map((lvl) => {
        const base = orderLevelKeyBase(lvl);
        const n = (counts.get(base) || 0) + 1;
        counts.set(base, n);
        return `${base}#${n}`;
    });
}

function mergeOwnOrdersIntoOrderbook(levels, side) {
    const marketId = Number(selectedMarketId);
    const merged = Array.isArray(levels) ? levels.map(level => ({ ...level })) : [];
    const seenOrderIds = new Set(
        merged
            .map(level => Number(level?.order_id))
            .filter(id => Number.isFinite(id) && id > 0)
    );
    const sideIsBuy = side === 'buy';
    const ownOrders = Array.isArray(ordersSnapshot)
        ? ordersSnapshot.filter(order =>
            Number(order.market_id) === marketId &&
            Boolean(order.buy) === sideIsBuy &&
            ACTIVE_ORDER_STATES.includes(getEffectiveOrderState(order)) &&
            Number(order.remaining_quantity || 0) > 0
        )
        : [];

    for (const order of ownOrders) {
        const orderId = Number(order.id);
        if (!Number.isFinite(orderId) || orderId <= 0) {
            continue;
        }
        if (seenOrderIds.has(orderId)) {
            continue;
        }
        const price = Number(order.price || 0);
        const quantity = Number(order.remaining_quantity || 0);
        if (!Number.isFinite(price) || price <= 0 || !Number.isFinite(quantity) || quantity <= 0) {
            continue;
        }
        let expiresInSeconds = null;
        if (order.expires_at) {
            const expiryMs = new Date(order.expires_at).getTime();
            if (Number.isFinite(expiryMs)) {
                expiresInSeconds = Math.max(0, Math.ceil((expiryMs - Date.now()) / 1000));
            }
        }
        merged.push({
            order_id: orderId,
            price,
            quantity,
            is_mine: true,
            cancel_token: `direct:${order.id}`,
            expires_in_seconds: expiresInSeconds
        });
        seenOrderIds.add(orderId);
    }

    merged.sort((a, b) => {
        const pa = Number(a?.price || 0);
        const pb = Number(b?.price || 0);
        if (sideIsBuy) {
            return pb - pa;
        }
        return pa - pb;
    });
    return merged;
}

function buildOrderLevelElement(level, side, orderKey, { animate } = { animate: false }) {
    const isMine = !!level.is_mine;
    const el = document.createElement('div');
    el.className = `order-level ${side} ${isMine ? 'own-order' : ''} ${animate ? 'order-insert' : ''}`;
    el.dataset.actionCancel = level.cancel_token || '';
    el.dataset.orderSide = side.toUpperCase();
    el.dataset.orderQty = String(level.quantity ?? '');
    el.dataset.orderPrice = String(level.price ?? '');
    el.dataset.orderKey = orderKey;

    const actionHtml = level.cancel_token
        ? `<button type="button" class="inline-cancel-btn btn-danger" data-action-cancel="${level.cancel_token}">Cancel</button>`
        : `<button type="button" class="inline-fill-btn btn-secondary">Fill</button>`;

    el.innerHTML = `
        <span>${formatPrice(level.price)}</span>
        <span>${formatInteger(level.quantity)}</span>
        <span>${formatPrice(level.value ?? (level.price * level.quantity))}</span>
        <span>${formatOrderLevelExpiry(level)}</span>
        <span class="order-action-cell">${actionHtml}</span>
    `;

    return el;
}

function updateOrderLevelElement(el, level, side, orderKey) {
    const isMine = !!level.is_mine;
    el.className = `order-level ${side} ${isMine ? 'own-order' : ''}`;
    el.dataset.actionCancel = level.cancel_token || '';
    el.dataset.orderSide = side.toUpperCase();
    el.dataset.orderQty = String(level.quantity ?? '');
    el.dataset.orderPrice = String(level.price ?? '');
    el.dataset.orderKey = orderKey;

    const spans = Array.from(el.querySelectorAll(':scope > span'));
    if (spans.length >= 4) {
        spans[0].textContent = formatPrice(level.price);
        spans[1].textContent = formatInteger(level.quantity);
        spans[2].textContent = formatPrice(level.value ?? (level.price * level.quantity));
        spans[3].textContent = formatOrderLevelExpiry(level);
    }

    const actionCell = spans[4] || el.querySelector('.order-action-cell');
    if (actionCell) {
        const actionHtml = level.cancel_token
            ? `<button type="button" class="inline-cancel-btn btn-danger" data-action-cancel="${level.cancel_token}">Cancel</button>`
            : `<button type="button" class="inline-fill-btn btn-secondary">Fill</button>`;
        actionCell.innerHTML = actionHtml;
        actionCell.classList.add('order-action-cell');
    }
}

function renderOrderbookSide(containerId, levels, side) {
    const container = document.getElementById(containerId);
    if (!container) return;

    const marketId = Number(selectedMarketId);
    const newKeys = listKeysWithOccurrence(levels);

    if (!orderbookState.initialized || Number(orderbookState.marketId) !== marketId) {
        orderbookState = { marketId, buys: [], sells: [], initialized: true };
    }

    const oldKeys = side === 'buy' ? orderbookState.buys : orderbookState.sells;

    if (levels.length === 0) {
        container.innerHTML = `<div style="text-align: center; color: #666; padding: 20px;">No ${side} orders</div>`;
        if (side === 'buy') orderbookState.buys = newKeys;
        else orderbookState.sells = newKeys;
        return;
    }

    // Reconcile by key: update existing rows in-place so qty/expiry/value stay consistent,
    // and move DOM nodes to match current orderbook ordering.
    const existingByKey = new Map();
    for (const child of Array.from(container.children)) {
        const key = child.dataset.orderKey;
        if (key) existingByKey.set(key, child);
    }

    const oldSet = new Set(oldKeys);
    const nodes = [];
    for (let i = 0; i < levels.length; i += 1) {
        const key = newKeys[i];
        const lvl = levels[i];
        const existing = existingByKey.get(key);
        if (existing) {
            updateOrderLevelElement(existing, lvl, side, key);
            nodes.push(existing);
        } else {
            const animate = oldKeys.length > 0 && !oldSet.has(key);
            nodes.push(buildOrderLevelElement(lvl, side, key, { animate }));
        }
    }

    container.replaceChildren(...nodes);

    if (side === 'buy') orderbookState.buys = newKeys;
    else orderbookState.sells = newKeys;
}

function renderOrderbookDeltaBar(data) {
    const buyTop = (data?.buys || [])[0];
    const sellTop = (data?.sells || [])[0];

    if (!buyTop || !sellTop) {
        setSignedStat('orderbookDeltaPrice', 0, '-');
        setSignedStat('orderbookDeltaQty', 0, '-');
        setSignedStat('orderbookDeltaValue', 0, '-');
        setSignedStat('orderbookDeltaExpiry', 0, '-');
        return;
    }

    const buyPrice = Number(buyTop.price || 0);
    const sellPrice = Number(sellTop.price || 0);
    const buyQty = Number(buyTop.quantity || 0);
    const sellQty = Number(sellTop.quantity || 0);
    const buyValue = Number(buyTop.value || (buyPrice * buyQty) || 0);
    const sellValue = Number(sellTop.value || (sellPrice * sellQty) || 0);

    const priceDelta = sellPrice - buyPrice;
    const qtyDelta = buyQty - sellQty;
    const valueDelta = buyValue - sellValue;

    const buyExpiry = Number(buyTop.expires_in_seconds);
    const sellExpiry = Number(sellTop.expires_in_seconds);
    const hasBuyExpiry = Number.isFinite(buyExpiry);
    const hasSellExpiry = Number.isFinite(sellExpiry);
    const expiryDelta = (hasBuyExpiry && hasSellExpiry) ? (buyExpiry - sellExpiry) : 0;
    const expiryText = (hasBuyExpiry && hasSellExpiry) ? formatSignedDuration(expiryDelta) : '-';

    setSignedStat('orderbookDeltaPrice', priceDelta, formatSignedPrice(priceDelta));
    setSignedStat('orderbookDeltaQty', qtyDelta, formatSignedInteger(qtyDelta));
    setSignedStat('orderbookDeltaValue', valueDelta, formatSignedPrice(valueDelta));
    setSignedStat('orderbookDeltaExpiry', expiryDelta, expiryText);
}

function computeBuyCapacityAtPrice(limitPrice, currencyBalance) {
    const sells = [...(latestOrderbook.sells || [])]
        .filter(level => Number(level.price) <= limitPrice)
        .sort((a, b) => Number(a.price) - Number(b.price));

    let remainingFunds = Number(currencyBalance || 0);
    let maxQty = 0;
    let availableQty = 0;

    for (const level of sells) {
        const price = Number(level.price || 0);
        const qty = Number(level.quantity || 0);
        if (price <= 0 || qty <= 0) continue;
        availableQty += qty;

        const affordableAtLevel = Math.floor(remainingFunds / price);
        if (affordableAtLevel <= 0) break;

        const takeQty = Math.min(qty, affordableAtLevel);
        maxQty += takeQty;
        remainingFunds -= takeQty * price;
    }

    return {
        affordableQty: Math.max(0, Math.floor(maxQty)),
        availableQty: Math.max(0, Math.floor(availableQty))
    };
}

function computeSellCapacityAtPrice(limitPrice, productBalance) {
    const buys = [...(latestOrderbook.buys || [])]
        .filter(level => Number(level.price) >= limitPrice)
        .sort((a, b) => Number(b.price) - Number(a.price));

    const availableQty = buys.reduce((sum, level) => sum + Number(level.quantity || 0), 0);
    const affordableQty = Math.max(0, Math.floor(Number(productBalance || 0)));

    return {
        affordableQty,
        availableQty: Math.max(0, Math.floor(availableQty))
    };
}

function bindTradePanelControls() {
    const openBtn = document.getElementById('openTradePanelBtn');
    const closeBtn = document.getElementById('closeTradePanelBtn');
    const sideEl = document.getElementById('tradeDirection');
    const priceEl = document.getElementById('tradePrice');
    const qtyEl = document.getElementById('tradeQuantity');
    const durationEl = document.getElementById('tradeDuration');
    const maxBtn = document.getElementById('tradeMaxBtn');
    if (!sideEl || !priceEl || !qtyEl || !durationEl || !maxBtn) return;

    if (openBtn && openBtn.dataset.boundClick !== 'true') {
        openBtn.addEventListener('click', () => {
            openTradePanel({ mode: 'limit', buy: true });
        });
        openBtn.dataset.boundClick = 'true';
    }
    if (closeBtn && closeBtn.dataset.boundClick !== 'true') {
        closeBtn.addEventListener('click', () => closeTradePanel());
        closeBtn.dataset.boundClick = 'true';
    }

    const onChange = () => syncTradePanelCapacityHint();
    for (const el of [sideEl, priceEl, qtyEl, durationEl]) {
        if (el.dataset.boundInput === 'true') continue;
        el.addEventListener('input', onChange);
        el.addEventListener('change', onChange);
        el.dataset.boundInput = 'true';
    }
    if (maxBtn.dataset.boundClick !== 'true') {
        maxBtn.addEventListener('click', () => applyTradePanelMaxQuantity());
        maxBtn.dataset.boundClick = 'true';
    }
}

function openTradePanel({ mode = 'limit', buy = true, price = null, quantity = null, sourceSide = null } = {}) {
    const panel = document.getElementById('tradePanel');
    const modeBadge = document.getElementById('tradeModeBadge');
    const modeHint = document.getElementById('tradeModeHint');
    const sideEl = document.getElementById('tradeDirection');
    const priceEl = document.getElementById('tradePrice');
    const qtyEl = document.getElementById('tradeQuantity');
    const durationEl = document.getElementById('tradeDuration');
    const submitBtn = document.getElementById('tradeSubmitBtn');
    if (!panel || !sideEl || !priceEl || !qtyEl || !durationEl || !modeBadge || !modeHint || !submitBtn) return;

    tradePanelState.open = true;
    tradePanelState.mode = mode === 'fill' ? 'fill' : 'limit';
    tradePanelState.sourceSide = sourceSide || null;

    sideEl.value = buy ? 'true' : 'false';
    if (Number.isFinite(price) && Number(price) > 0) {
        priceEl.value = Number(price).toFixed(2);
    } else if (!priceEl.value) {
        const fallback = buy ? Number(latestOrderbook.sells?.[0]?.price) : Number(latestOrderbook.buys?.[0]?.price);
        if (Number.isFinite(fallback) && fallback > 0) priceEl.value = fallback.toFixed(2);
    }
    if (Number.isFinite(quantity) && Number(quantity) > 0) {
        qtyEl.value = String(Math.floor(Number(quantity)));
    }
    if (!durationEl.value) durationEl.value = tradePanelState.mode === 'fill' ? '1' : '';
    if (tradePanelState.mode === 'fill') durationEl.value = '1';

    modeBadge.textContent = tradePanelState.mode === 'fill' ? 'FILL' : 'LIMIT';
    modeHint.textContent = tradePanelState.mode === 'fill'
        ? 'Completing visible opposite-side liquidity'
        : 'Manual order entry';
    submitBtn.textContent = tradePanelState.mode === 'fill' ? 'Place Fill' : 'Place Order';
    panel.classList.remove('hidden');
    syncTradePanelCapacityHint();
}

function closeTradePanel() {
    const panel = document.getElementById('tradePanel');
    if (!panel) return;
    tradePanelState.open = false;
    tradePanelState.mode = 'limit';
    tradePanelState.sourceSide = null;
    panel.classList.add('hidden');
}

function getTradePanelInputs() {
    const sideEl = document.getElementById('tradeDirection');
    const priceEl = document.getElementById('tradePrice');
    const qtyEl = document.getElementById('tradeQuantity');
    const durationEl = document.getElementById('tradeDuration');
    return { sideEl, priceEl, qtyEl, durationEl };
}

async function ensureWalletSnapshotFresh() {
    // Keep the market header + trade panel responsive without reloading /orders/me.
    return refreshMarketWalletHoldings();
}

async function refreshMarketWalletHoldings({ force = false } = {}) {
    if (!authToken) return walletSnapshot;

    const now = Date.now();
    if (!force && (now - marketWalletLastFetchMs) < MARKET_WALLET_REFRESH_MS) {
        return walletSnapshot;
    }
    if (marketWalletInFlight) {
        return marketWalletInFlight;
    }

    marketWalletLastFetchMs = now;
    marketWalletInFlight = (async () => {
        const data = await apiRequest('/users/me/wallet');
        walletSnapshot = data;
        renderCurrentMarketWalletHoldings();
        syncTradePanelCapacityHint();
        return walletSnapshot;
    })().finally(() => {
        marketWalletInFlight = null;
    });

    return marketWalletInFlight;
}

function computeTradePanelCapacities({ buy, price, wallet }) {
    const [product, currency] = (selectedMarket || '').split('/');
    if (!product || !currency || !wallet) return { maxQty: 0, affordableQty: 0, availableQty: 0, reason: 'Select a market.' };
    if (!Number.isFinite(price) || price <= 0) return { maxQty: 0, affordableQty: 0, availableQty: 0, reason: 'Enter a valid price.' };

    const availCurrency = Number(wallet.available?.currencies?.[currency] ?? wallet.currencies?.[currency] ?? 0);
    const availProduct = Number(wallet.available?.products?.[product] ?? wallet.products?.[product] ?? 0);

    if (tradePanelState.mode === 'fill') {
        const capacities = buy
            ? computeBuyCapacityAtPrice(price, availCurrency)
            : computeSellCapacityAtPrice(price, availProduct);
        const maxQty = Math.max(0, Math.min(capacities.affordableQty, capacities.availableQty));
        return {
            maxQty,
            affordableQty: capacities.affordableQty,
            availableQty: capacities.availableQty,
            reason: maxQty > 0 ? '' : (buy
                ? `No executable quantity at ${formatPrice(price)} with ${currency} balance and visible sells.`
                : `No executable quantity at ${formatPrice(price)} with ${product} balance and visible buys.`)
        };
    }

    // Limit/manual mode: max by wallet position.
    if (buy) {
        const affordableQty = Math.max(0, Math.floor(availCurrency / price));
        return { maxQty: affordableQty, affordableQty, availableQty: Number.MAX_SAFE_INTEGER, reason: affordableQty > 0 ? '' : `Insufficient ${currency}.` };
    }
    const affordableQty = Math.max(0, Math.floor(availProduct));
    return { maxQty: affordableQty, affordableQty, availableQty: Number.MAX_SAFE_INTEGER, reason: affordableQty > 0 ? '' : `Insufficient ${product}.` };
}

async function syncTradePanelCapacityHint() {
    const hintEl = document.getElementById('tradeCapacityHint');
    const { sideEl, priceEl, qtyEl } = getTradePanelInputs();
    if (!hintEl || !sideEl || !priceEl || !qtyEl || !tradePanelState.open) return;

    try {
        const wallet = await ensureWalletSnapshotFresh();
        const buy = sideEl.value === 'true';
        const price = Number(priceEl.value);
        const cap = computeTradePanelCapacities({ buy, price, wallet });
        if (cap.maxQty > 0) {
            const parts = [`Max: ${formatInteger(cap.maxQty)}`];
            if (tradePanelState.mode === 'fill') {
                parts.push(`afford ${formatInteger(cap.affordableQty)}`);
                parts.push(`book ${formatInteger(cap.availableQty)}`);
            }
            hintEl.textContent = parts.join(' | ');
        } else {
            hintEl.textContent = `Max: 0${cap.reason ? ` (${cap.reason})` : ''}`;
        }

        const currentQty = Number(qtyEl.value);
        if (Number.isFinite(currentQty) && currentQty > cap.maxQty && cap.maxQty > 0) {
            qtyEl.value = String(cap.maxQty);
        }
    } catch {
        hintEl.textContent = 'Max: -';
    }
}

async function applyTradePanelMaxQuantity() {
    const { sideEl, priceEl, qtyEl } = getTradePanelInputs();
    if (!sideEl || !priceEl || !qtyEl) return;
    const wallet = await ensureWalletSnapshotFresh();
    const buy = sideEl.value === 'true';
    const price = Number(priceEl.value);
    const cap = computeTradePanelCapacities({ buy, price, wallet });
    if (cap.maxQty > 0) {
        qtyEl.value = String(cap.maxQty);
    }
    syncTradePanelCapacityHint();
}

function bindInlineOrderActionButtons() {
    document.querySelectorAll('.inline-cancel-btn').forEach((button) => {
        if (button.dataset.boundClick === 'true') {
            return;
        }
        button.addEventListener('click', async (event) => {
            event.preventDefault();
            event.stopPropagation();
            const cancelToken = button.dataset.actionCancel;
            if (!cancelToken) return;
            if (!confirm('Cancel this order?')) return;
            button.disabled = true;
            try {
                if (cancelToken.startsWith('direct:')) {
                    const orderId = Number(cancelToken.slice('direct:'.length));
                    if (!Number.isFinite(orderId) || orderId <= 0) {
                        throw new Error('invalid order id');
                    }
                    await apiRequest(`/orders/${orderId}`, { method: 'DELETE' });
                } else {
                    await apiRequest('/orders/actions/cancel', {
                        method: 'POST',
                        body: JSON.stringify({ token: cancelToken })
                    });
                }
                showResult('placeOrderResult', 'Order cancelled.');
                await loadMarketData();
                loadSystemStats();
                loadMyWallet();
            } catch (error) {
                showResult('placeOrderResult', 'Unable to cancel order.', true, error.message);
                button.disabled = false;
            }
        });
        button.dataset.boundClick = 'true';
    });

    document.querySelectorAll('.inline-fill-btn').forEach((button) => {
        if (button.dataset.boundClick === 'true') {
            return;
        }
        button.addEventListener('click', async (event) => {
            event.preventDefault();
            event.stopPropagation();
            const row = button.closest('.order-level');
            if (!row) return;

            const orderSide = row.dataset.orderSide;
            const orderPrice = Number(row.dataset.orderPrice || 0);
            const orderQty = Number(row.dataset.orderQty || 0);
            if (!orderSide || orderPrice <= 0) {
                showResult('placeOrderResult', 'Unable to determine fill details.', true);
                return;
            }
            const buy = orderSide === 'SELL';
            openTradePanel({
                mode: 'fill',
                buy,
                price: orderPrice,
                quantity: Number.isFinite(orderQty) && orderQty > 0 ? orderQty : null,
                sourceSide: orderSide,
            });
        });
        button.dataset.boundClick = 'true';
    });
}

async function loadRecentTrades() {
    try {
        const data = await apiRequest(`/markets/${selectedMarketId}/trades?limit=250`);
        const trades = Array.isArray(data?.trades) ? data.trades : [];
        lastRecentTradesByMarket.set(Number(selectedMarketId), trades);
        renderRecentTrades(trades.slice(0, 30));
        ingestMarketPriceTrades(trades);
        ingestMarketExecutionTrades(trades);
        renderMarketPriceChart();
        renderMarketVolumeChart();
        renderMarketValueChart();
    } catch (error) {
        console.error('Error loading trades:', error);
        const fallback = lastRecentTradesByMarket.get(Number(selectedMarketId)) || [];
        if (fallback.length > 0) {
            renderRecentTrades(fallback.slice(0, 30));
            ingestMarketPriceTrades(fallback);
            ingestMarketExecutionTrades(fallback);
        }
        renderMarketPriceChart();
        renderMarketVolumeChart();
        renderMarketValueChart();
    }
}

function tradeKey(trade) {
    const tsMs = Math.round(Number(trade.timestamp || 0) * 1000);
    const price = Number(trade.price || 0).toFixed(2);
    const qty = Math.trunc(Number(trade.quantity || 0));
    const side = String(trade.my_side || '');
    return `${tsMs}|${price}|${qty}|${side}`;
}

function ensureMarketPriceChartWiring() {
    const canvas = document.getElementById('marketPriceChart');
    if (!canvas) return;

    const container = canvas.closest('.market-chart-container') || canvas.parentElement;
    if (!container) return;

    if (marketPriceChartResizeObserver) {
        return;
    }

    marketPriceChartResizeObserver = new ResizeObserver(() => {
        renderMarketPriceChart();
    });
    marketPriceChartResizeObserver.observe(container);
}

function resetMarketPriceChart(marketId) {
    marketPriceChartState = {
        marketId,
        points: [],
        seenKeys: new Set(),
        initialized: true
    };
    const last = document.getElementById('chartLastPrice');
    if (last) last.textContent = '-';
    renderMarketPriceChart();
}

function ingestMarketPriceTrades(trades) {
    const marketId = Number(selectedMarketId);
    if (!marketId) return;

    if (!marketPriceChartState.initialized || Number(marketPriceChartState.marketId) !== marketId) {
        resetMarketPriceChart(marketId);
    }

    const nowMs = Date.now();
    const cutoffMs = nowMs - 60_000;

    for (const trade of trades) {
        const key = tradeKey(trade);
        if (marketPriceChartState.seenKeys.has(key)) continue;

        const tMs = Math.round(Number(trade.timestamp || 0) * 1000);
        if (!Number.isFinite(tMs) || tMs < cutoffMs - 2_000) continue;

        const price = Number(trade.price);
        if (!Number.isFinite(price)) continue;

        marketPriceChartState.seenKeys.add(key);
        marketPriceChartState.points.push({ key, tMs, price });
    }

    // Prune to last minute and keep seenKeys bounded.
    marketPriceChartState.points = marketPriceChartState.points.filter(p => p.tMs >= cutoffMs);
    marketPriceChartState.points.sort((a, b) => a.tMs - b.tMs);
    marketPriceChartState.seenKeys = new Set(marketPriceChartState.points.map(p => p.key));
}

function renderMarketPriceChart() {
    const canvas = document.getElementById('marketPriceChart');
    if (!canvas) return;

    const marketId = Number(selectedMarketId);
    const points = (marketPriceChartState.initialized && Number(marketPriceChartState.marketId) === marketId)
        ? marketPriceChartState.points
        : [];

    const cssW = Math.max(0, Math.floor(canvas.clientWidth));
    const cssH = Math.max(0, Math.floor(canvas.clientHeight));
    if (cssW === 0 || cssH === 0) return;

    const dpr = window.devicePixelRatio || 1;
    const w = cssW;
    const h = cssH;
    const pxW = Math.max(1, Math.floor(w * dpr));
    const pxH = Math.max(1, Math.floor(h * dpr));

    if (canvas.width !== pxW) canvas.width = pxW;
    if (canvas.height !== pxH) canvas.height = pxH;

    const ctx = canvas.getContext('2d');
    if (!ctx) return;

    ctx.save();
    ctx.scale(dpr, dpr);

    // Background.
    ctx.clearRect(0, 0, w, h);
    ctx.fillStyle = '#0f0f23';
    ctx.fillRect(0, 0, w, h);

    const nowMs = Date.now();
    const cutoffMs = nowMs - 60_000;

    const lastPriceEl = document.getElementById('chartLastPrice');
    const lastPoint = points.length ? points[points.length - 1] : null;
    if (lastPriceEl) {
        lastPriceEl.textContent = lastPoint ? formatPrice(lastPoint.price) : '-';
    }

    // Grid.
    ctx.strokeStyle = 'rgba(45, 45, 68, 0.8)';
    ctx.lineWidth = 1;
    ctx.beginPath();
    for (let i = 0; i <= 4; i += 1) {
        const x = (w * i) / 4;
        ctx.moveTo(x + 0.5, 0);
        ctx.lineTo(x + 0.5, h);
    }
    for (let i = 0; i <= 3; i += 1) {
        const y = (h * i) / 3;
        ctx.moveTo(0, y + 0.5);
        ctx.lineTo(w, y + 0.5);
    }
    ctx.stroke();

    const drawLeftLabel = (y, text, edge = 'mid') => {
        ctx.fillStyle = 'rgba(183, 190, 208, 0.62)';
        ctx.font = '600 10px -apple-system, BlinkMacSystemFont, Segoe UI, Roboto, Helvetica Neue, Arial, sans-serif';
        ctx.textAlign = 'left';
        ctx.textBaseline = edge === 'top' ? 'top' : edge === 'bottom' ? 'bottom' : 'middle';
        ctx.fillText(text, 6, y);
    };

    if (points.length === 0) {
        ctx.fillStyle = 'rgba(224, 224, 224, 0.55)';
        ctx.font = '600 12px -apple-system, BlinkMacSystemFont, Segoe UI, Roboto, Helvetica Neue, Arial, sans-serif';
        ctx.textAlign = 'center';
        ctx.textBaseline = 'middle';
        ctx.fillText('No trades in last minute', w / 2, h / 2);
        ctx.restore();
        return;
    }

    // Build 1s candles for the last minute.
    const nowSec = Math.floor(nowMs / 1000);
    const cutoffSec = nowSec - 59;
    const candlesBySec = new Map();

    for (const p of points) {
        const sec = Math.floor(p.tMs / 1000);
        if (sec < cutoffSec || sec > nowSec) continue;
        const c = candlesBySec.get(sec);
        if (!c) {
            candlesBySec.set(sec, { sec, open: p.price, high: p.price, low: p.price, close: p.price });
        } else {
            if (p.price > c.high) c.high = p.price;
            if (p.price < c.low) c.low = p.price;
            c.close = p.price;
        }
    }

    const candles = [];
    for (let sec = cutoffSec; sec <= nowSec; sec += 1) {
        const c = candlesBySec.get(sec);
        if (c) candles.push(c);
    }

    if (candles.length === 0) {
        ctx.fillStyle = 'rgba(224, 224, 224, 0.55)';
        ctx.font = '600 12px -apple-system, BlinkMacSystemFont, Segoe UI, Roboto, Helvetica Neue, Arial, sans-serif';
        ctx.textAlign = 'center';
        ctx.textBaseline = 'middle';
        ctx.fillText('No trades in last minute', w / 2, h / 2);
        ctx.restore();
        return;
    }

    let minP = Infinity;
    let maxP = -Infinity;
    for (const c of candles) {
        if (c.low < minP) minP = c.low;
        if (c.high > maxP) maxP = c.high;
    }

    if (!Number.isFinite(minP) || !Number.isFinite(maxP)) {
        ctx.restore();
        return;
    }

    if (minP === maxP) {
        const pad = Math.max(0.01, minP * 0.003);
        minP -= pad;
        maxP += pad;
    } else {
        const pad = (maxP - minP) * 0.16;
        minP -= pad;
        maxP += pad;
    }

    const bucketW = w / 60;
    const bodyW = Math.max(2, Math.min(bucketW * 0.62, 10));
    const plotTop = 14;
    const plotBottom = h - 14;
    const plotHeight = Math.max(1, plotBottom - plotTop);
    const toY = (price) => plotBottom - ((price - minP) / (maxP - minP)) * plotHeight;
    const xForSec = (sec) => ((sec - cutoffSec) + 0.5) * bucketW;

    // Discreet y-axis labels on gridlines.
    for (let i = 0; i <= 3; i += 1) {
        const y = plotTop + (plotHeight * i) / 3;
        const v = maxP - ((maxP - minP) * (i / 3));
        const edge = i === 0 ? 'top' : i === 3 ? 'bottom' : 'mid';
        drawLeftLabel(y + 0.5, formatPrice(v), edge);
    }

    // Wicks.
    ctx.strokeStyle = 'rgba(185, 191, 206, 0.75)';
    ctx.lineWidth = 1;
    ctx.beginPath();
    for (const c of candles) {
        const x = xForSec(c.sec);
        ctx.moveTo(x + 0.5, toY(c.high));
        ctx.lineTo(x + 0.5, toY(c.low));
    }
    ctx.stroke();

    // Bodies.
    for (const c of candles) {
        const x = xForSec(c.sec);
        const yOpen = toY(c.open);
        const yClose = toY(c.close);
        const top = Math.min(yOpen, yClose);
        const bottom = Math.max(yOpen, yClose);
        const height = Math.max(1, bottom - top);

        const up = c.close >= c.open;
        ctx.fillStyle = up ? 'rgba(76, 175, 80, 0.85)' : 'rgba(244, 67, 54, 0.85)';
        ctx.strokeStyle = up ? 'rgba(76, 175, 80, 0.95)' : 'rgba(244, 67, 54, 0.95)';
        ctx.lineWidth = 1;

        const left = x - bodyW / 2;
        if (height <= 1.2) {
            // Doji: draw a thin line.
            ctx.beginPath();
            ctx.moveTo(left, top + 0.5);
            ctx.lineTo(left + bodyW, top + 0.5);
            ctx.stroke();
        } else {
            ctx.fillRect(left, top, bodyW, height);
        }
    }

    // Overlay: close-price line (carry-forward across missing seconds).
    const closeSeries = [];
    let lastClose = candles[0].open;
    const candlesBySecDense = new Map(candles.map(c => [c.sec, c]));
    for (let sec = cutoffSec; sec <= nowSec; sec += 1) {
        const c = candlesBySecDense.get(sec);
        if (c) lastClose = c.close;
        closeSeries.push({ sec, close: lastClose });
    }

    ctx.strokeStyle = 'rgba(90, 200, 250, 0.55)';
    ctx.lineWidth = 1.6;
    ctx.lineJoin = 'round';
    ctx.lineCap = 'round';
    ctx.beginPath();
    for (let i = 0; i < closeSeries.length; i += 1) {
        const p = closeSeries[i];
        const x = xForSec(p.sec);
        const y = toY(p.close);
        if (i === 0) ctx.moveTo(x, y);
        else ctx.lineTo(x, y);
    }
    ctx.stroke();

    // Last close marker.
    const lastClosePoint = closeSeries[closeSeries.length - 1];
    ctx.fillStyle = 'rgba(90, 200, 250, 0.65)';
    ctx.beginPath();
    ctx.arc(xForSec(lastClosePoint.sec), toY(lastClosePoint.close), 2.4, 0, Math.PI * 2);
    ctx.fill();

    // A subtle fade on the left edge so the line doesn't pop in/out.
    const fade = ctx.createLinearGradient(0, 0, 18, 0);
    fade.addColorStop(0, '#0f0f23');
    fade.addColorStop(1, 'rgba(15, 15, 35, 0)');
    ctx.fillStyle = fade;
    ctx.fillRect(0, 0, 18, h);

    ctx.restore();
}

function ensureMarketPulseChartWiring() {
    const canvas = document.getElementById('marketPulseChart');
    if (!canvas) return;
    const container = canvas.closest('.market-chart-container') || canvas.parentElement;
    if (!container) return;

    if (marketPulseChartResizeObserver) {
        return;
    }
    marketPulseChartResizeObserver = new ResizeObserver(() => {
        renderMarketPulseChart();
    });
    marketPulseChartResizeObserver.observe(container);
}

function resetMarketPulseChart(marketId) {
    marketPulseState = {
        marketId,
        points: [],
        initialized: true
    };
    const spreadEl = document.getElementById('pulseSpreadValue');
    const imbalanceEl = document.getElementById('pulseImbalanceValue');
    if (spreadEl) spreadEl.textContent = '-';
    if (imbalanceEl) imbalanceEl.textContent = '-';
    renderMarketPulseChart();
}

function resetMarketExecutionCharts(marketId) {
    marketExecutionSeriesState = {
        marketId,
        points: [],
        seenKeys: new Set(),
        initialized: true
    };
    const volumeEl = document.getElementById('volumeLastMinuteValue');
    const valueEl = document.getElementById('valueLastMinuteValue');
    if (volumeEl) volumeEl.textContent = '-';
    if (valueEl) valueEl.textContent = '-';
    renderMarketVolumeChart();
    renderMarketValueChart();
}

function ingestMarketPulseSnapshot(orderbookData) {
    const marketId = Number(selectedMarketId);
    if (!marketId) return;
    if (!marketPulseState.initialized || Number(marketPulseState.marketId) !== marketId) {
        resetMarketPulseChart(marketId);
    }

    const buyTop = (orderbookData?.buys || [])[0];
    const sellTop = (orderbookData?.sells || [])[0];
    const spread = (buyTop && sellTop)
        ? Math.max(0.01, Number(sellTop.price || 0) - Number(buyTop.price || 0))
        : 0;
    const totalQty = Number(marketSummary.totalBuyQty || 0) + Number(marketSummary.totalSellQty || 0);
    const imbalance = totalQty > 0
        ? (Number(marketSummary.totalBuyQty || 0) - Number(marketSummary.totalSellQty || 0)) / totalQty
        : 0;

    const nowMs = Date.now();
    marketPulseState.points.push({ tMs: nowMs, spread, imbalance });
    const cutoffMs = nowMs - 60_000;
    marketPulseState.points = marketPulseState.points.filter(p => p.tMs >= cutoffMs);

    const spreadEl = document.getElementById('pulseSpreadValue');
    const imbalanceEl = document.getElementById('pulseImbalanceValue');
    if (spreadEl) spreadEl.textContent = formatPrice(spread);
    if (imbalanceEl) imbalanceEl.textContent = `${imbalance >= 0 ? '+' : ''}${(imbalance * 100).toFixed(1)}%`;
}

function renderMarketPulseChart() {
    const canvas = document.getElementById('marketPulseChart');
    if (!canvas) return;

    const marketId = Number(selectedMarketId);
    const points = (marketPulseState.initialized && Number(marketPulseState.marketId) === marketId)
        ? marketPulseState.points
        : [];

    const cssW = Math.max(0, Math.floor(canvas.clientWidth));
    const cssH = Math.max(0, Math.floor(canvas.clientHeight));
    if (cssW === 0 || cssH === 0) return;

    const dpr = window.devicePixelRatio || 1;
    const pxW = Math.max(1, Math.floor(cssW * dpr));
    const pxH = Math.max(1, Math.floor(cssH * dpr));
    if (canvas.width !== pxW) canvas.width = pxW;
    if (canvas.height !== pxH) canvas.height = pxH;

    const ctx = canvas.getContext('2d');
    if (!ctx) return;
    ctx.save();
    ctx.scale(dpr, dpr);
    ctx.clearRect(0, 0, cssW, cssH);
    ctx.fillStyle = '#0f0f23';
    ctx.fillRect(0, 0, cssW, cssH);

    // Grid
    ctx.strokeStyle = 'rgba(45, 45, 68, 0.75)';
    ctx.lineWidth = 1;
    ctx.beginPath();
    for (let i = 0; i <= 4; i += 1) {
        const x = (cssW * i) / 4;
        ctx.moveTo(x + 0.5, 0);
        ctx.lineTo(x + 0.5, cssH);
    }
    for (let i = 0; i <= 3; i += 1) {
        const y = (cssH * i) / 3;
        ctx.moveTo(0, y + 0.5);
        ctx.lineTo(cssW, y + 0.5);
    }
    ctx.stroke();

    const drawLeftLabel = (y, text, edge = 'mid') => {
        ctx.fillStyle = 'rgba(183, 190, 208, 0.58)';
        ctx.font = '600 10px -apple-system, BlinkMacSystemFont, Segoe UI, Roboto, Helvetica Neue, Arial, sans-serif';
        ctx.textAlign = 'left';
        ctx.textBaseline = edge === 'top' ? 'top' : edge === 'bottom' ? 'bottom' : 'middle';
        ctx.fillText(text, 6, y);
    };

    if (!points.length) {
        ctx.fillStyle = 'rgba(224, 224, 224, 0.55)';
        ctx.font = '600 12px -apple-system, BlinkMacSystemFont, Segoe UI, Roboto, Helvetica Neue, Arial, sans-serif';
        ctx.textAlign = 'center';
        ctx.textBaseline = 'middle';
        ctx.fillText('Waiting for market snapshots', cssW / 2, cssH / 2);
        ctx.restore();
        return;
    }

    const nowMs = Date.now();
    const minT = nowMs - 60_000;
    const spreads = points.map(p => Number(p.spread || 0));
    const imbalances = points.map(p => Number(p.imbalance || 0));
    const maxSpread = Math.max(...spreads, 0.0001);
    const maxImb = Math.max(...imbalances.map(v => Math.abs(v)), 0.001);
    const toX = (tMs) => ((Math.max(minT, tMs) - minT) / 60_000) * cssW;
    const plotTop = 14;
    const plotBottom = cssH - 14;
    const plotHeight = Math.max(1, plotBottom - plotTop);
    const spreadY = (s) => plotBottom - (Math.min(maxSpread, Math.max(0, s)) / maxSpread) * plotHeight;
    const imbY = (v) => plotBottom - ((Math.max(-maxImb, Math.min(maxImb, v)) + maxImb) / (2 * maxImb)) * plotHeight;

    // Right-side labels for spread scale (top/mid/bottom).
    drawLeftLabel(plotTop + 0.5, formatPrice(maxSpread), 'top');
    drawLeftLabel((plotTop + plotBottom) / 2 + 0.5, formatPrice(maxSpread / 2), 'mid');
    drawLeftLabel(plotBottom + 0.5, formatPrice(0), 'bottom');

    // Spread line
    ctx.strokeStyle = 'rgba(255, 193, 7, 0.90)';
    ctx.lineWidth = 1.7;
    ctx.beginPath();
    points.forEach((p, idx) => {
        const x = toX(p.tMs);
        const y = spreadY(Number(p.spread || 0));
        if (idx === 0) ctx.moveTo(x, y);
        else ctx.lineTo(x, y);
    });
    ctx.stroke();

    // Imbalance line
    ctx.strokeStyle = 'rgba(90, 200, 250, 0.82)';
    ctx.lineWidth = 1.5;
    ctx.beginPath();
    points.forEach((p, idx) => {
        const x = toX(p.tMs);
        const y = imbY(Number(p.imbalance || 0));
        if (idx === 0) ctx.moveTo(x, y);
        else ctx.lineTo(x, y);
    });
    ctx.stroke();

    ctx.restore();
}

function buildTradeElement(trade, { animate } = { animate: false }) {
    const date = new Date(trade.timestamp * 1000);
    const mySide = String(trade.my_side || '').toUpperCase();
    const mineBySide = mySide === 'BUY' || mySide === 'SELL';
    const sideClass = !mineBySide
        ? ''
        : mySide === 'BUY'
            ? 'own-trade-buy'
            : mySide === 'SELL'
                ? 'own-trade-sell'
                : '';

    const el = document.createElement('div');
    el.className = `trade-item ${mineBySide ? 'own-trade' : ''} ${sideClass} ${animate ? 'trade-insert' : ''}`;
    el.dataset.tradeKey = tradeKey(trade);
    el.dataset.tradePrice = String(Number(trade.price) || 0);
    el.dataset.tradeMine = mineBySide ? '1' : '0';
    el.dataset.tradeSide = mySide;
    el.innerHTML = `
        <div class="trade-time">${date.toLocaleTimeString()}</div>
        <div class="trade-price">${formatPrice(trade.price)}</div>
        <div class="trade-qty">${formatInteger(trade.quantity)}</div>
        <div class="trade-value">${formatPrice(trade.value ?? (trade.price * trade.quantity))}</div>
    `;
    return el;
}

function renderRecentTrades(trades) {
    const container = document.getElementById('recentTrades');
    if (!container) return;

    const marketId = Number(selectedMarketId);
    const newKeys = trades.map(tradeKey);

    // First render or market switch: render all, no animations.
    if (!recentTradesState.initialized || Number(recentTradesState.marketId) !== marketId) {
        recentTradesState = { marketId, keys: newKeys, initialized: true };
        if (trades.length === 0) {
            container.innerHTML = '<div style="text-align: center; color: #666; padding: 20px;">No recent trades</div>';
            return;
        }
        container.replaceChildren(...trades.map(t => buildTradeElement(t, { animate: false })));
        updateRecentTradePriceColours(container);
        return;
    }

    const oldKeys = recentTradesState.keys;
    const oldFirst = oldKeys[0];
    const overlapIndex = oldFirst ? newKeys.indexOf(oldFirst) : -1;

    // If we can't find overlap (or huge changes), do a full re-render without animation.
    if (overlapIndex < 0 || overlapIndex > 12) {
        recentTradesState.keys = newKeys;
        if (trades.length === 0) {
            container.innerHTML = '<div style="text-align: center; color: #666; padding: 20px;">No recent trades</div>';
            return;
        }
        container.replaceChildren(...trades.map(t => buildTradeElement(t, { animate: false })));
        updateRecentTradePriceColours(container);
        return;
    }

    const inserted = trades.slice(0, overlapIndex);
    if (inserted.length === 0) {
        // Nothing new at the top; keep DOM and just update keys.
        recentTradesState.keys = newKeys;
        return;
    }

    // Insert new trades at top with animation. Iterate from oldest->newest so ordering stays correct.
    for (let i = inserted.length - 1; i >= 0; i -= 1) {
        const el = buildTradeElement(inserted[i], { animate: true });
        container.insertBefore(el, container.firstChild);
    }

    // Trim any excess beyond limit and remove placeholder nodes.
    const keepSet = new Set(newKeys);
    const children = Array.from(container.children);
    for (const child of children) {
        const key = child.dataset.tradeKey;
        if (!key || !keepSet.has(key)) {
            container.removeChild(child);
        }
    }
    while (container.children.length > newKeys.length) {
        container.removeChild(container.lastChild);
    }

    recentTradesState.keys = newKeys;
    updateRecentTradePriceColours(container);
}

function updateRecentTradePriceColours(container) {
    const items = Array.from(container.querySelectorAll('.trade-item'));
    for (let i = 0; i < items.length; i += 1) {
        const curr = items[i];
        const next = items[i + 1];
        const currVal = Number(curr.dataset.tradePrice);
        const nextVal = next ? Number(next.dataset.tradePrice) : Number.NaN;

        const valueEl = curr.querySelector('.trade-price');
        if (!valueEl) continue;
        valueEl.classList.remove('value-up', 'value-down', 'value-flat', 'mine-buy', 'mine-sell');

        // Keep strict buy/sell colouring for own trades.
        if (curr.dataset.tradeMine === '1') {
            const side = String(curr.dataset.tradeSide || '').toUpperCase();
            if (side === 'BUY') {
                valueEl.classList.add('mine-buy');
                continue;
            }
            if (side === 'SELL') {
                valueEl.classList.add('mine-sell');
                continue;
            }
        }

        if (!Number.isFinite(currVal) || !Number.isFinite(nextVal)) {
            valueEl.classList.add('value-flat');
            continue;
        }

        if (currVal > nextVal) valueEl.classList.add('value-up');
        else if (currVal < nextVal) valueEl.classList.add('value-down');
        else valueEl.classList.add('value-flat');
    }
}

async function loadMarketStats() {
    try {
        const data = await apiRequest(`/markets/${selectedMarketId}/stats`);

        marketSummary.activeBuys = Number(data.active_buy_orders || 0);
        marketSummary.activeSells = Number(data.active_sell_orders || 0);
        renderMarketSummary();
    } catch (error) {
        console.error('Error loading market stats:', error);
    }
}

function renderMarketSummary() {
    document.getElementById('activeBuys').textContent = formatInteger(marketSummary.activeBuys);
    document.getElementById('activeSells').textContent = formatInteger(marketSummary.activeSells);

    document.getElementById('totalBuyQtyValue').textContent = formatInteger(marketSummary.totalBuyQty);
    document.getElementById('totalSellQtyValue').textContent = formatInteger(marketSummary.totalSellQty);
    document.getElementById('totalQueueQtyValue').textContent = formatInteger(marketSummary.totalBuyQty + marketSummary.totalSellQty);
    document.getElementById('totalBuyValue').textContent = formatPrice(marketSummary.totalBuyValue);
    document.getElementById('totalSellValue').textContent = formatPrice(marketSummary.totalSellValue);
    document.getElementById('totalBookValue').textContent = formatPrice(marketSummary.totalBuyValue + marketSummary.totalSellValue);

    const qtyDelta = marketSummary.totalBuyQty - marketSummary.totalSellQty;
    const orderDelta = marketSummary.activeBuys - marketSummary.activeSells;
    const valueDelta = marketSummary.totalBuyValue - marketSummary.totalSellValue;

    setSignedStat('qtyDeltaValue', qtyDelta, formatSignedInteger(qtyDelta));
    setSignedStat('orderDeltaValue', orderDelta, formatSignedInteger(orderDelta));
    setSignedStat('valueDeltaValue', valueDelta, formatSignedPrice(valueDelta));
    setSignedStat('pressureValue', valueDelta, formatSignedCompact(valueDelta));
}

function ensureMarketVolumeChartWiring() {
    const canvas = document.getElementById('marketVolumeChart');
    if (!canvas) return;
    const container = canvas.closest('.market-chart-container') || canvas.parentElement;
    if (!container || marketVolumeChartResizeObserver) return;
    marketVolumeChartResizeObserver = new ResizeObserver(() => renderMarketVolumeChart());
    marketVolumeChartResizeObserver.observe(container);
}

function ensureMarketValueChartWiring() {
    const canvas = document.getElementById('marketValueChart');
    if (!canvas) return;
    const container = canvas.closest('.market-chart-container') || canvas.parentElement;
    if (!container || marketValueChartResizeObserver) return;
    marketValueChartResizeObserver = new ResizeObserver(() => renderMarketValueChart());
    marketValueChartResizeObserver.observe(container);
}

function ingestMarketExecutionTrades(trades) {
    const marketId = Number(selectedMarketId);
    if (!marketId) return;
    if (!marketExecutionSeriesState.initialized || Number(marketExecutionSeriesState.marketId) !== marketId) {
        resetMarketExecutionCharts(marketId);
    }
    const nowMs = Date.now();
    const cutoffMs = nowMs - 60_000;
    for (const trade of trades) {
        const key = tradeKey(trade);
        if (marketExecutionSeriesState.seenKeys.has(key)) continue;
        const tMs = Math.round(Number(trade.timestamp || 0) * 1000);
        if (!Number.isFinite(tMs) || tMs < cutoffMs - 2_000) continue;
        const qty = Math.max(0, Number(trade.quantity || 0));
        const value = Math.max(0, Number(trade.value || (Number(trade.price || 0) * qty)));
        marketExecutionSeriesState.seenKeys.add(key);
        marketExecutionSeriesState.points.push({ key, tMs, qty, value });
    }
    marketExecutionSeriesState.points = marketExecutionSeriesState.points.filter(p => p.tMs >= cutoffMs);
    marketExecutionSeriesState.points.sort((a, b) => a.tMs - b.tMs);
    marketExecutionSeriesState.seenKeys = new Set(marketExecutionSeriesState.points.map(p => p.key));
}

function getExecutionBuckets(field) {
    const nowMs = Date.now();
    const nowSec = Math.floor(nowMs / 1000);
    const cutoffSec = nowSec - 59;
    const points = marketExecutionSeriesState.points || [];
    const buckets = new Array(60).fill(0);
    for (const p of points) {
        const sec = Math.floor(Number(p.tMs || 0) / 1000);
        if (sec < cutoffSec || sec > nowSec) continue;
        const idx = sec - cutoffSec;
        buckets[idx] += Math.max(0, Number(p[field] || 0));
    }
    return buckets;
}

function renderMinuteBarChart(canvas, buckets, color, emptyText, valueFormatter) {
    if (!canvas) return;
    const cssW = Math.max(0, Math.floor(canvas.clientWidth));
    const cssH = Math.max(0, Math.floor(canvas.clientHeight));
    if (cssW === 0 || cssH === 0) return;

    const dpr = window.devicePixelRatio || 1;
    const pxW = Math.max(1, Math.floor(cssW * dpr));
    const pxH = Math.max(1, Math.floor(cssH * dpr));
    if (canvas.width !== pxW) canvas.width = pxW;
    if (canvas.height !== pxH) canvas.height = pxH;

    const ctx = canvas.getContext('2d');
    if (!ctx) return;
    ctx.save();
    ctx.scale(dpr, dpr);
    ctx.clearRect(0, 0, cssW, cssH);
    ctx.fillStyle = '#0f0f23';
    ctx.fillRect(0, 0, cssW, cssH);

    ctx.strokeStyle = 'rgba(45, 45, 68, 0.7)';
    ctx.lineWidth = 1;
    ctx.beginPath();
    for (let i = 0; i <= 3; i += 1) {
        const y = (cssH * i) / 3;
        ctx.moveTo(0, y + 0.5);
        ctx.lineTo(cssW, y + 0.5);
    }
    ctx.stroke();

    const maxV = Math.max(...buckets, 0);
    if (maxV <= 0) {
        ctx.fillStyle = 'rgba(224, 224, 224, 0.55)';
        ctx.font = '600 12px -apple-system, BlinkMacSystemFont, Segoe UI, Roboto, Helvetica Neue, Arial, sans-serif';
        ctx.textAlign = 'center';
        ctx.textBaseline = 'middle';
        ctx.fillText(emptyText, cssW / 2, cssH / 2);
        ctx.restore();
        return;
    }

    const drawLeftLabel = (y, text, edge = 'mid') => {
        ctx.fillStyle = 'rgba(183, 190, 208, 0.58)';
        ctx.font = '600 10px -apple-system, BlinkMacSystemFont, Segoe UI, Roboto, Helvetica Neue, Arial, sans-serif';
        ctx.textAlign = 'left';
        ctx.textBaseline = edge === 'top' ? 'top' : edge === 'bottom' ? 'bottom' : 'middle';
        ctx.fillText(text, 6, y);
    };
    drawLeftLabel(6, valueFormatter(maxV), 'top');
    drawLeftLabel(cssH - 6, valueFormatter(0), 'bottom');

    const plotTop = 14;
    const plotBottom = cssH - 14;
    const plotHeight = Math.max(1, plotBottom - plotTop);
    const toY = (v) => plotBottom - (Math.max(0, v) / maxV) * plotHeight;
    const bucketW = cssW / 60;
    const barW = Math.max(1, Math.min(bucketW - 1, 6));

    ctx.fillStyle = color;
    for (let i = 0; i < buckets.length; i += 1) {
        const v = Number(buckets[i] || 0);
        if (v <= 0) continue;
        const x = (i * bucketW) + ((bucketW - barW) / 2);
        const y = toY(v);
        const h = Math.max(1, plotBottom - y);
        ctx.fillRect(x, y, barW, h);
    }
    ctx.restore();
}

function renderMarketVolumeChart() {
    const buckets = getExecutionBuckets('qty');
    const total = buckets.reduce((s, v) => s + Number(v || 0), 0);
    const totalEl = document.getElementById('volumeLastMinuteValue');
    if (totalEl) totalEl.textContent = formatInteger(total);
    renderMinuteBarChart(
        document.getElementById('marketVolumeChart'),
        buckets,
        'rgba(76, 175, 80, 0.88)',
        'No volume in last minute',
        (v) => formatInteger(v)
    );
}

function renderMarketValueChart() {
    const buckets = getExecutionBuckets('value');
    const total = buckets.reduce((s, v) => s + Number(v || 0), 0);
    const totalEl = document.getElementById('valueLastMinuteValue');
    if (totalEl) totalEl.textContent = formatPrice(total);
    renderMinuteBarChart(
        document.getElementById('marketValueChart'),
        buckets,
        'rgba(33, 150, 243, 0.86)',
        'No value in last minute',
        (v) => formatPrice(v)
    );
}

// Unified Trade Panel
async function submitTradePanel() {
    const { sideEl, priceEl, qtyEl, durationEl } = getTradePanelInputs();
    if (!sideEl || !priceEl || !qtyEl || !durationEl) return;
    if (!selectedMarketId) {
        showResult('placeOrderResult', 'Select a market first.', true);
        return;
    }

    const buy = sideEl.value === 'true';
    const quantity = Math.floor(Number(qtyEl.value));
    const price = Number(priceEl.value);
    const durationRaw = String(durationEl.value ?? '').trim();
    let expiresInSeconds = null;
    if (durationRaw.length > 0) {
        const parsed = Math.floor(Number(durationRaw));
        if (!Number.isFinite(parsed) || parsed < 1) {
            showResult('placeOrderResult', 'Duration must be at least 1 second, or leave it blank for no expiry.', true);
            return;
        }
        expiresInSeconds = parsed;
    }

    if (!Number.isFinite(quantity) || quantity <= 0 || !Number.isFinite(price) || price <= 0) {
        showResult('placeOrderResult', 'Enter valid quantity and price.', true);
        return;
    }

    try {
        const wallet = await ensureWalletSnapshotFresh();
        const cap = computeTradePanelCapacities({ buy, price, wallet });
        if (quantity > cap.maxQty) {
            showResult('placeOrderResult', `Quantity exceeds max executable size (${formatInteger(cap.maxQty)}).`, true);
            return;
        }

        const payload = {
            market_id: selectedMarketId,
            quantity,
            price: price.toString(),
            buy
        };
        if (expiresInSeconds != null) {
            payload.expires_in_seconds = expiresInSeconds;
        }

        await apiRequest('/orders', {
            method: 'POST',
            body: JSON.stringify(payload)
        });

        showResult(
            'placeOrderResult',
            `${tradePanelState.mode === 'fill' ? 'Fill' : 'Order'} placed.`,
            false,
            `Side: ${buy ? 'BUY' : 'SELL'} | Qty: ${formatInteger(quantity)} | Price: ${formatPrice(price)}`
        );

        qtyEl.value = '';
        if (tradePanelState.mode === 'fill') {
            closeTradePanel();
        }

        loadSystemStats();
        loadMarketData();
        loadMyWallet();
    } catch (error) {
        showResult('placeOrderResult', 'Unable to place order.', true, error.message);
    }
}

// Backward-compatible entry point.
async function placeOrder() {
    return submitTradePanel();
}

// My Wallet
async function loadMyWallet() {
    try {
        const [wallet, details] = await Promise.all([
            apiRequest('/users/me/wallet'),
            apiRequest('/users/me/wallet/details')
        ]);
        walletSnapshot = wallet;
        walletDetailsSnapshot = details;
        renderWalletBalances();
        renderWalletDetailMetrics();
        renderWalletInsights();
        renderCurrentMarketWalletHoldings();
        await loadMyOrders('walletOrdersList', 'walletOrdersResult');
    } catch (error) {
        console.error('Error loading wallet:', error);
    }
}

function renderCurrentMarketWalletHoldings() {
    const productEl = document.getElementById('marketWalletProduct');
    const currencyEl = document.getElementById('marketWalletCurrency');
    if (!productEl || !currencyEl) return;

    if (!selectedMarket || !selectedMarket.includes('/')) {
        productEl.textContent = 'Product: -';
        currencyEl.textContent = 'Currency: -';
        return;
    }

    const [product, currency] = selectedMarket.split('/');
    const wallet = walletSnapshot || { currencies: {}, products: {}, available: { currencies: {}, products: {} } };
    const productAmount = Number(wallet.available?.products?.[product] ?? wallet.products?.[product] ?? 0);
    const currencyAmount = Number(wallet.available?.currencies?.[currency] ?? wallet.currencies?.[currency] ?? 0);

    productEl.textContent = `${product}: ${formatInteger(productAmount)}`;
    currencyEl.textContent = `${currency}: ${formatPrice(currencyAmount)}`;
}

// My Orders
async function loadMyOrders(listElementId = 'myOrdersList', resultElementId = 'myOrdersResult', providedOrders = null) {
    try {
        const orders = Array.isArray(providedOrders) ? providedOrders : await apiRequest('/orders/me');
        ordersSnapshot = orders;
        orderMatchesCache = new Map();
        const activeOrders = orders
            .filter(order => ACTIVE_ORDER_STATES.includes(getEffectiveOrderState(order)))
            .sort((a, b) => b.id - a.id);
        const historicalOrders = orders
            .filter(order => !ACTIVE_ORDER_STATES.includes(getEffectiveOrderState(order)))
            .sort((a, b) => b.id - a.id);

        const container = document.getElementById(listElementId);
        if (!container) {
            return;
        }
        container.innerHTML = orders.length > 0
            ? `
                ${renderOrderSection(`Active Orders (${formatInteger(activeOrders.length)})`, activeOrders)}
                ${renderOrderSection('Order History', historicalOrders)}
            `
            : '<p style="text-align: center; color: #666; padding: 20px;">No orders found</p>';

        renderWalletBalances();
        renderWalletInsights();
        startExpiryTicker();
    } catch (error) {
        console.error('Error loading orders:', error);
        const resultElement = document.getElementById(resultElementId);
        if (resultElement) {
            showResult(resultElementId, 'Unable to load your orders.', true, error.message);
        }
    }
}

async function refreshMyOrdersSnapshot({ force = false } = {}) {
    if (!authToken) return ordersSnapshot;

    const now = Date.now();
    if (!force && (now - myOrdersLastFetchMs) < MY_ORDERS_REFRESH_MS) {
        return ordersSnapshot;
    }
    if (myOrdersInFlight) {
        return myOrdersInFlight;
    }

    myOrdersLastFetchMs = now;
    myOrdersInFlight = (async () => {
        const orders = await apiRequest('/orders/me');
        ordersSnapshot = Array.isArray(orders) ? orders : [];
        if (isWalletTabActive()) {
            await loadMyOrders('walletOrdersList', 'walletOrdersResult', ordersSnapshot);
        }
        return ordersSnapshot;
    })().finally(() => {
        myOrdersInFlight = null;
    });

    return myOrdersInFlight;
}

function computeRecentWalletFlows(lookbackHours = 24) {
    const orders = Array.isArray(ordersSnapshot) ? ordersSnapshot : [];
    const currencyDelta = {};
    const productDelta = {};
    const now = Date.now();
    const cutoffMs = now - (lookbackHours * 3600 * 1000);

    for (const order of orders) {
        const createdMs = new Date(order.created_at).getTime();
        if (!Number.isFinite(createdMs) || createdMs < cutoffMs) {
            continue;
        }

        const filledQty = Math.max(0, Number(order.quantity || 0) - Number(order.remaining_quantity || 0));
        if (filledQty <= 0) {
            continue;
        }

        const ticker = marketsById.get(order.market_id);
        if (!ticker || !ticker.includes('/')) {
            continue;
        }
        const [product, currency] = ticker.split('/');
        const orderPrice = Number(order.price || 0);
        if (!product || !currency || orderPrice <= 0) {
            continue;
        }

        if (order.buy) {
            productDelta[product] = (productDelta[product] || 0) + filledQty;
            currencyDelta[currency] = (currencyDelta[currency] || 0) - (orderPrice * filledQty);
        } else {
            productDelta[product] = (productDelta[product] || 0) - filledQty;
            currencyDelta[currency] = (currencyDelta[currency] || 0) + (orderPrice * filledQty);
        }
    }

    return { currencyDelta, productDelta };
}

function renderWalletBalances() {
    const wallet = walletSnapshot || { currencies: {}, products: {} };
    const { currencyDelta, productDelta } = computeRecentWalletFlows(24);

    const currencyContainer = document.getElementById('currencyBalances');
    currencyContainer.innerHTML = Object.entries(wallet.currencies || {}).length > 0
        ? Object.entries(wallet.currencies).map(([currency, amount]) => {
            const delta = Number(currencyDelta[currency] || 0);
            const deltaClass = delta > 0 ? 'delta-positive' : delta < 0 ? 'delta-negative' : 'delta-neutral';
            const deltaText = delta === 0 ? '' : `<span class="balance-change ${deltaClass}">(${formatSignedPrice(delta)})</span>`;
            return `
                <div class="balance-item">
                    <span class="balance-currency">${currency}</span>
                    <span class="balance-amount">${deltaText} ${formatPrice(amount)}</span>
                </div>
            `;
        }).join('')
        : '<p style="text-align: center; color: #666; padding: 20px;">No currency balances</p>';

    const productContainer = document.getElementById('productBalances');
    productContainer.innerHTML = Object.entries(wallet.products || {}).length > 0
        ? Object.entries(wallet.products).map(([product, amount]) => {
            const delta = Number(productDelta[product] || 0);
            const deltaClass = delta > 0 ? 'delta-positive' : delta < 0 ? 'delta-negative' : 'delta-neutral';
            const deltaText = delta === 0 ? '' : `<span class="balance-change ${deltaClass}">(${formatSignedInteger(delta)})</span>`;
            return `
                <div class="balance-item">
                    <span class="balance-currency">${product}</span>
                    <span class="balance-amount">${deltaText} ${formatInteger(amount)}</span>
                </div>
            `;
        }).join('')
        : '<p style="text-align: center; color: #666; padding: 20px;">No product balances</p>';
}

function renderWalletDetailMetrics() {
    const container = document.getElementById('walletDetailMetrics');
    if (!container) return;

    const details = walletDetailsSnapshot || {};
    const currencies = details?.balances?.currencies || {};
    const products = details?.balances?.products || {};
    const currencyEntries = Object.entries(currencies).sort(([a], [b]) => a.localeCompare(b));
    const productEntries = Object.entries(products).sort(([a], [b]) => a.localeCompare(b));
    const cardCount = Math.max(4, currencyEntries.length, productEntries.length);

    const cards = Array.from({ length: cardCount }).map((_, i) => {
        const currencyEntry = currencyEntries[i] || null;
        const productEntry = productEntries[i] || null;

        let currencyBlock = `
            <div class="wallet-metric-row wallet-metric-empty">
                <span class="wallet-metric-label">Currency</span>
                <span class="wallet-metric-value wallet-metric-neutral">No data</span>
            </div>
        `;
        if (currencyEntry) {
            const [code, row] = currencyEntry;
            const deposited = Number(row.deposited || 0);
            const withdrawn = Number(row.withdrawn || 0);
            const feesPaid = Number(row.fees_paid || 0);
            const netFlow = Number(row.net_flow || (deposited - withdrawn));
            const netClass = netFlow > 0 ? 'wallet-metric-positive' : netFlow < 0 ? 'wallet-metric-negative' : 'wallet-metric-neutral';
            currencyBlock = `
                <div class="wallet-metric-title">${code} Cashflow</div>
                <div class="wallet-metric-row">
                    <span class="wallet-metric-label">Deposited</span>
                    <span class="wallet-metric-value wallet-metric-positive">+${formatPrice(deposited)}</span>
                </div>
                <div class="wallet-metric-row">
                    <span class="wallet-metric-label">Withdrawn</span>
                    <span class="wallet-metric-value wallet-metric-negative">-${formatPrice(withdrawn)}</span>
                </div>
                <div class="wallet-metric-row">
                    <span class="wallet-metric-label">Fees Paid</span>
                    <span class="wallet-metric-value wallet-metric-warning">${formatPrice(feesPaid)}</span>
                </div>
                <div class="wallet-metric-row wallet-metric-net">
                    <span class="wallet-metric-label">Net Flow</span>
                    <span class="wallet-metric-value ${netClass}">${formatSignedPrice(netFlow)}</span>
                </div>
            `;
        }

        let productBlock = `
            <div class="wallet-metric-row wallet-metric-empty">
                <span class="wallet-metric-label">Product</span>
                <span class="wallet-metric-value wallet-metric-neutral">No data</span>
            </div>
        `;
        if (productEntry) {
            const [code, row] = productEntry;
            const deposited = Number(row.deposited || 0);
            const withdrawn = Number(row.withdrawn || 0);
            const netFlow = Number(row.net_flow || (deposited - withdrawn));
            const netClass = netFlow > 0 ? 'wallet-metric-positive' : netFlow < 0 ? 'wallet-metric-negative' : 'wallet-metric-neutral';
            productBlock = `
                <div class="wallet-metric-title">${code} Units</div>
                <div class="wallet-metric-row">
                    <span class="wallet-metric-label">Deposited</span>
                    <span class="wallet-metric-value wallet-metric-positive">+${formatInteger(deposited)}</span>
                </div>
                <div class="wallet-metric-row">
                    <span class="wallet-metric-label">Withdrawn</span>
                    <span class="wallet-metric-value wallet-metric-negative">-${formatInteger(withdrawn)}</span>
                </div>
                <div class="wallet-metric-row wallet-metric-net">
                    <span class="wallet-metric-label">Net Flow</span>
                    <span class="wallet-metric-value ${netClass}">${formatSignedInteger(netFlow)}</span>
                </div>
            `;
        }

        return `
            <div class="wallet-metric-card wallet-metric-combined">
                ${currencyBlock}
                <div class="wallet-metric-divider"></div>
                ${productBlock}
            </div>
        `;
    });

    container.innerHTML = cards.length
        ? `<div class="wallet-metric-grid">${cards.join('')}</div>`
        : '<div style="color: #666;">No detail data available.</div>';
}

async function changeMyPassword() {
    const currentPassword = String(document.getElementById('walletCurrentPassword')?.value || '').trim();
    const newPassword = String(document.getElementById('walletNewPassword')?.value || '').trim();
    if (!currentPassword || !newPassword) {
        showResult('walletPasswordResult', 'Enter current and new password.', true);
        return;
    }
    try {
        await apiRequest('/auth/change-password', {
            method: 'POST',
            body: JSON.stringify({
                current_password: currentPassword,
                new_password: newPassword
            })
        });
        document.getElementById('walletCurrentPassword').value = '';
        document.getElementById('walletNewPassword').value = '';
        showResult('walletPasswordResult', 'Password updated.');
    } catch (error) {
        showResult('walletPasswordResult', 'Unable to update password.', true, error.message);
    }
}

async function withdrawMyCurrency() {
    const currency = String(document.getElementById('walletWithdrawCurrency')?.value || '').trim().toUpperCase();
    const amount = String(document.getElementById('walletWithdrawCurrencyAmount')?.value || '').trim();
    if (!currency || !amount || Number(amount) <= 0) {
        showResult('walletWithdrawCurrencyResult', 'Enter valid currency and amount.', true);
        return;
    }
    try {
        await apiRequest('/users/me/wallet/withdraw/currency', {
            method: 'POST',
            body: JSON.stringify({ currency, amount })
        });
        showResult('walletWithdrawCurrencyResult', `Withdrawal submitted (${currency} ${formatPrice(amount)}).`);
        await loadMyWallet();
    } catch (error) {
        showResult('walletWithdrawCurrencyResult', 'Unable to withdraw currency.', true, error.message);
    }
}

async function withdrawMyProduct() {
    const product = String(document.getElementById('walletWithdrawProduct')?.value || '').trim().toUpperCase();
    const amountUnits = Math.floor(Number(document.getElementById('walletWithdrawProductUnits')?.value || 0));
    if (!product || !Number.isFinite(amountUnits) || amountUnits <= 0) {
        showResult('walletWithdrawProductResult', 'Enter valid product and units.', true);
        return;
    }
    try {
        await apiRequest('/users/me/wallet/withdraw/product', {
            method: 'POST',
            body: JSON.stringify({ product, amount_units: amountUnits })
        });
        showResult('walletWithdrawProductResult', `Withdrawal submitted (${formatInteger(amountUnits)} ${product}).`);
        await loadMyWallet();
    } catch (error) {
        showResult('walletWithdrawProductResult', 'Unable to withdraw product.', true, error.message);
    }
}

function renderWalletInsights() {
    const container = document.getElementById('walletInsights');
    if (!container) return;

    const orders = Array.isArray(ordersSnapshot) ? ordersSnapshot : [];
    const activeOrders = orders.filter(o => ACTIVE_ORDER_STATES.includes(getEffectiveOrderState(o)));
    const now = Date.now();

    const expiringSoonCount = activeOrders.filter(o => {
        if (!o.expires_at) return false;
        const expiry = new Date(o.expires_at).getTime();
        return Number.isFinite(expiry) && expiry > now && expiry <= (now + 3600 * 1000);
    }).length;
    const completedCount = orders.filter(o => getEffectiveOrderState(o) === 'FILLED').length;
    const cancelledCount = orders.filter(o => getEffectiveOrderState(o) === 'CANCELLED').length;
    const expiredCount = orders.filter(o => getEffectiveOrderState(o) === 'EXPIRED').length;

    const items = [
        ['Active', formatInteger(activeOrders.length)],
        ['Completed Orders', formatInteger(completedCount)],
        ['Cancelled', formatInteger(cancelledCount)],
        ['Expired', formatInteger(expiredCount)],
        ['Expire < 1h', formatInteger(expiringSoonCount)]
    ];

    container.innerHTML = items.map(([label, value]) => `
        <div class="balance-item insight-item">
            <span class="balance-currency">${label}</span>
            <span class="balance-amount">${value}</span>
        </div>
    `).join('');
}

function renderOrderSection(title, orders) {
    if (!orders || orders.length === 0) {
        return `
            <div class="orders-section">
                <h3 class="orders-section-title">${title}</h3>
                <p style="text-align: center; color: #666; padding: 12px 0;">No orders</p>
            </div>
        `;
    }

    return `
        <div class="orders-section">
            <h3 class="orders-section-title">${title}</h3>
            ${orders.map(order => {
                const state = getEffectiveOrderState(order);
                const canCancel = state === 'OPEN' || state === 'PARTIAL';
                const displayState = state === 'PENDING_SUBMIT' ? 'PENDING' : state;
                return `
                    <div class="order-item wallet-order-item" onclick="toggleOrderMatches(${order.id})">
                        <div class="order-field">
                            <div class="order-label">Side</div>
                            <div class="direction-${order.buy ? 'buy' : 'sell'}">${order.buy ? 'BUY' : 'SELL'}</div>
                        </div>
                        <div class="order-field">
                            <div class="order-label">Size</div>
                            <div class="order-value">${formatInteger(order.remaining_quantity)}/${formatInteger(order.quantity)}</div>
                        </div>
                        <div class="order-field">
                            <div class="order-label">Price</div>
                            <div class="order-value">${formatPrice(order.price)}</div>
                        </div>
                        <div class="order-field">
                            <div class="order-label">State</div>
                            <div class="order-value">${displayState}</div>
                        </div>
                        <div class="order-field">
                            <div class="order-label">Market</div>
                            <div class="order-meta">${getMarketTicker(order.market_id)}</div>
                        </div>
                        <div class="order-field">
                            <div class="order-label">Expires</div>
                            <div class="order-meta">${renderOrderExpiry(order.expires_at)}</div>
                        </div>
                        <div class="order-actions">
                            <button class="btn-secondary" onclick="event.stopPropagation(); toggleOrderMatches(${order.id})">Matches</button>
                            <button class="btn-danger" onclick="event.stopPropagation(); cancelOrder(${order.id})" ${canCancel ? '' : 'disabled'}>Cancel</button>
                        </div>
                    </div>
                    <div class="order-matches" id="orderMatches-${order.id}" style="display: none;"></div>
                `;
            }).join('')}
        </div>
    `;
}

async function toggleOrderMatches(orderId) {
    const container = document.getElementById(`orderMatches-${orderId}`);
    if (!container) return;

    if (container.style.display === 'block') {
        container.style.display = 'none';
        return;
    }

    container.style.display = 'block';
    container.innerHTML = '<div class="order-meta">Loading matches...</div>';

    try {
        let data = orderMatchesCache.get(orderId);
        if (!data) {
            data = await apiRequest(`/orders/${orderId}/matches`);
            orderMatchesCache.set(orderId, data);
        }

        if (!data.matches || data.matches.length === 0) {
            container.innerHTML = '<div class="order-meta">Matches: 0</div>';
            return;
        }

        const orderSide = String(data.side || '').toUpperCase() || '-';
        container.innerHTML = `
            <div class="order-meta">Matches: ${formatInteger(data.matches.length)} | Side: ${orderSide}</div>
            <div class="order-matches-list">
                <div class="order-match-row order-match-header">
                    <span>Time</span>
                    <span>Side</span>
                    <span>Qty</span>
                    <span>Price</span>
                    <span>Value</span>
                </div>
                ${data.matches.map(match => {
                    const date = new Date(match.timestamp * 1000);
                    return `
                        <div class="order-match-row">
                            <span>${date.toLocaleTimeString()}</span>
                            <span>${orderSide}</span>
                            <span>${formatInteger(match.quantity)}</span>
                            <span>${formatPrice(match.price)}</span>
                            <span>${formatPrice(Number(match.price || 0) * Number(match.quantity || 0))}</span>
                        </div>
                    `;
                }).join('')}
            </div>
        `;
    } catch (error) {
        container.innerHTML = `<div class="order-meta">Unable to load matches: ${error.message}</div>`;
    }
}

async function cancelOrder(orderId) {
    if (!confirm(`Cancel order #${orderId}?`)) return;

    try {
        await apiRequest(`/orders/${orderId}`, { method: 'DELETE' });
        const walletResult = document.getElementById('walletOrdersResult');
        if (walletResult) {
            showResult('walletOrdersResult', `Cancelled order #${orderId}.`);
        }
        await loadMyWallet();
        loadMarketData();
        loadSystemStats();
    } catch (error) {
        const walletResult = document.getElementById('walletOrdersResult');
        if (walletResult) {
            showResult('walletOrdersResult', `Unable to cancel order #${orderId}.`, true, error.message);
        }
    }
}

// Admin Functions
async function adminLoadPerf() {
    try {
        const data = await apiRequest('/admin/debug/perf');
        const container = document.getElementById('adminPerfSnapshot');
        if (container) {
            container.innerHTML = `<pre>${JSON.stringify(data, null, 2)}</pre>`;
        }
    } catch (error) {
        showResult('adminDiagnosticsResult', 'Unable to load perf snapshot.', true, error.message);
    }
}

async function adminLoadTxJournal() {
    try {
        const data = await apiRequest('/admin/debug/tx-journal?limit=200');
        const container = document.getElementById('adminTxJournalSnapshot');
        if (container) {
            container.innerHTML = `<pre>${JSON.stringify(data, null, 2)}</pre>`;
        }
    } catch (error) {
        showResult('adminDiagnosticsResult', 'Unable to load tx journal.', true, error.message);
    }
}

async function adminDevTopup() {
    const minUserId = Math.floor(Number(document.getElementById('adminTopupMinUserId')?.value || 1));
    const currencyAmount = String(document.getElementById('adminTopupCurrencyAmount')?.value || '').trim();
    const productUnits = Math.floor(Number(document.getElementById('adminTopupProductUnits')?.value || 0));

    if (!Number.isFinite(minUserId) || minUserId < 1) {
        showResult('adminTopupResult', 'Min user id must be >= 1.', true);
        return;
    }
    if (!currencyAmount || !Number.isFinite(Number(currencyAmount)) || Number(currencyAmount) <= 0) {
        showResult('adminTopupResult', 'Currency amount must be > 0.', true);
        return;
    }
    if (!Number.isFinite(productUnits) || productUnits <= 0) {
        showResult('adminTopupResult', 'Product units must be > 0.', true);
        return;
    }

    try {
        const data = await apiRequest('/admin/dev/topup', {
            method: 'POST',
            body: JSON.stringify({
                min_user_id: minUserId,
                currency_amount: currencyAmount,
                // Rust endpoint expects `product_amount` (units for each product).
                // Keep server-side aliases too, but send canonical name here.
                product_amount: productUnits
            })
        });
        showResult(
            'adminTopupResult',
            `Topup queued.`,
            false,
            `Events written: ${formatInteger(data.events_written || 0)}`
        );
    } catch (error) {
        showResult('adminTopupResult', 'Unable to topup users.', true, error.message);
    }
}

async function adminLoadFeeConfig() {
    try {
        const data = await apiRequest('/admin/fees');
        const el = document.getElementById('adminFeeRate');
        if (el) el.value = (Number(data.seller_fee_rate) || 0).toFixed(6);
    } catch (error) {
        showResult('adminFeeConfigResult', 'Unable to load fee config.', true, error.message);
    }
}

async function adminSetFeeConfig() {
    const rateStr = String(document.getElementById('adminFeeRate')?.value || '').trim();
    const rateNum = Number(rateStr);
    if (!Number.isFinite(rateNum) || rateNum < 0 || rateNum > 1) {
        showResult('adminFeeConfigResult', 'Fee rate must be between 0 and 1 (e.g. 0.005).', true);
        return;
    }
    try {
        await apiRequest('/admin/fees', {
            method: 'POST',
            body: JSON.stringify({ seller_fee_rate: rateStr })
        });
        showResult('adminFeeConfigResult', 'Fee rate updated.');
        adminGetFeesCollected();
    } catch (error) {
        showResult('adminFeeConfigResult', 'Unable to update fee rate.', true, error.message);
    }
}

async function adminGetFeesCollected() {
    try {
        const data = await apiRequest('/admin/fees/collected');
        const totals = data.totals || {};
        const entries = Object.entries(totals).sort(([a], [b]) => a.localeCompare(b));

        const list = document.getElementById('adminFeesCollectedList');
        if (list) {
            list.innerHTML = entries.length
                ? entries.map(([currency, amount]) => `
                    <div class="stat-item">
                        <span class="stat-label">${currency}</span>
                        <span class="stat-value">${formatPrice(amount)}</span>
                    </div>
                `).join('')
                : '<div style="color: #666;">No fees collected yet.</div>';
        }

        const updated = document.getElementById('adminFeesCollectedUpdated');
        if (updated) {
            updated.textContent = data.updated_at
                ? `Updated ${new Date(data.updated_at * 1000).toLocaleTimeString()}`
                : 'Updated -';
        }
    } catch (error) {
        showResult('adminFeesCollectedResult', 'Unable to fetch fees collected.', true, error.message);
    }
}

function adminResolveSelectedUserId() {
    const inputId = Math.floor(Number(document.getElementById('adminSelectedUserId')?.value || 0));
    if (Number.isFinite(inputId) && inputId > 0) {
        adminSelectedUserId = inputId;
    }
    return adminSelectedUserId;
}

function adminRenderUsersList() {
    const container = document.getElementById('adminUsersList');
    if (!container) return;
    if (!Array.isArray(adminUsersSnapshot) || adminUsersSnapshot.length === 0) {
        container.innerHTML = '<div style="color:#888;">No users found.</div>';
        return;
    }
    container.innerHTML = adminUsersSnapshot.map((u) => `
        <button type="button" class="admin-user-row ${Number(adminSelectedUserId) === Number(u.user_id) ? 'active' : ''}" onclick="adminSelectUser(${u.user_id})">
            <span>#${u.user_id} ${u.username}</span>
            <span>${u.is_admin ? 'ADMIN' : 'USER'}</span>
        </button>
    `).join('');
}

function adminPopulateSelectedUserForm(user) {
    if (!user) return;
    const idEl = document.getElementById('adminSelectedUserId');
    const userEl = document.getElementById('adminSelectedUsername');
    const emailEl = document.getElementById('adminSelectedEmail');
    const adminEl = document.getElementById('adminSelectedIsAdmin');
    const banner = document.getElementById('adminSelectedUserBanner');
    if (idEl) idEl.value = user.user_id;
    if (userEl) userEl.value = user.username || '';
    if (emailEl) emailEl.value = user.email || '';
    if (adminEl) adminEl.value = String(!!user.is_admin);
    if (banner) banner.textContent = `Selected #${user.user_id} ${user.username} (${user.is_admin ? 'admin' : 'user'})`;
}

function adminRenderSelectedUserSnapshot() {
    const statsEl = document.getElementById('adminSelectedUserStats');
    const walletEl = document.getElementById('adminSelectedUserWallet');
    if (!statsEl || !walletEl) return;
    const snap = adminSelectedUserAccount;
    if (!snap) {
        statsEl.innerHTML = '<div style="color:#888;">No snapshot loaded.</div>';
        walletEl.textContent = '';
        return;
    }
    const outOrders = Array.isArray(snap?.orders?.outstanding) ? snap.orders.outstanding : [];
    const doneOrders = Array.isArray(snap?.orders?.completed) ? snap.orders.completed : [];
    const recentTrades = Array.isArray(snap?.recent_trades) ? snap.recent_trades : [];
    const outCount = outOrders.length;
    const doneCount = doneOrders.length;
    const tradeCount = recentTrades.length;
    statsEl.innerHTML = `
        <div class="stat-item"><span class="stat-label">Outstanding Orders</span><span class="stat-value">${formatInteger(outCount)}</span></div>
        <div class="stat-item"><span class="stat-label">Completed Orders</span><span class="stat-value">${formatInteger(doneCount)}</span></div>
        <div class="stat-item"><span class="stat-label">Recent Trades</span><span class="stat-value">${formatInteger(tradeCount)}</span></div>
    `;

    const currencies = Object.entries(snap?.wallet?.currencies || {}).sort(([a], [b]) => a.localeCompare(b));
    const products = Object.entries(snap?.wallet?.products || {}).sort(([a], [b]) => a.localeCompare(b));

    const renderCompactRows = (rows, type) => rows.slice(0, 20).map(([code, row]) => {
        const total = type === 'currency' ? formatPrice(row.total) : formatInteger(row.total);
        const available = type === 'currency' ? formatPrice(row.available) : formatInteger(row.available);
        const reserved = type === 'currency' ? formatPrice(row.reserved) : formatInteger(row.reserved);
        return `<div class="stat-item"><span class="stat-label">${code}</span><span class="stat-value">T ${total} | A ${available} | R ${reserved}</span></div>`;
    }).join('');

    const renderOrderRows = (orders) => orders.slice(0, 12).map((o) => `
        <div class="stat-item">
            <span class="stat-label">#${o.order_id} ${o.buy ? 'BUY' : 'SELL'} ${getMarketTicker(o.market_id)}</span>
            <span class="stat-value">${formatInteger(o.remaining)}/${formatInteger(o.quantity)} @ ${formatPrice(o.price)} (${o.status})</span>
        </div>
    `).join('');

    const renderTradeRows = (trades) => trades.slice(0, 12).map((t) => `
        <div class="stat-item">
            <span class="stat-label">${new Date(Number(t.timestamp || 0) * 1000).toLocaleTimeString()} ${t.side} ${getMarketTicker(t.market_id)}</span>
            <span class="stat-value">${formatInteger(t.quantity)} @ ${formatPrice(t.price)} (fee ${formatPrice(t.fee)})</span>
        </div>
    `).join('');

    walletEl.innerHTML = `
        <div class="admin-snapshot-grid">
            <div>
                <h4 class="orders-section-title">Wallet Currencies</h4>
                <div class="admin-fees-list">${currencies.length ? renderCompactRows(currencies, 'currency') : '<div style="color:#888;">No currencies</div>'}</div>
            </div>
            <div>
                <h4 class="orders-section-title">Wallet Products</h4>
                <div class="admin-fees-list">${products.length ? renderCompactRows(products, 'product') : '<div style="color:#888;">No products</div>'}</div>
            </div>
        </div>
        <div class="admin-snapshot-grid" style="margin-top:12px;">
            <div>
                <h4 class="orders-section-title">Outstanding Orders</h4>
                <div class="admin-fees-list">${outOrders.length ? renderOrderRows(outOrders) : '<div style="color:#888;">No outstanding orders</div>'}</div>
            </div>
            <div>
                <h4 class="orders-section-title">Completed Orders</h4>
                <div class="admin-fees-list">${doneOrders.length ? renderOrderRows(doneOrders) : '<div style="color:#888;">No completed orders</div>'}</div>
            </div>
        </div>
        <div style="margin-top:12px;">
            <h4 class="orders-section-title">Recent Trades</h4>
            <div class="admin-fees-list">${recentTrades.length ? renderTradeRows(recentTrades) : '<div style="color:#888;">No recent trades</div>'}</div>
        </div>
    `;
}

async function adminLoadUsers() {
    try {
        const users = await apiRequest('/admin/users');
        adminUsersSnapshot = Array.isArray(users) ? users : [];
        adminRenderUsersList();
        if (!adminSelectedUserId && adminUsersSnapshot.length > 0) {
            await adminSelectUser(adminUsersSnapshot[0].user_id);
        } else if (adminSelectedUserId) {
            const selected = adminUsersSnapshot.find((u) => Number(u.user_id) === Number(adminSelectedUserId));
            if (selected) {
                adminPopulateSelectedUserForm(selected);
            }
        }
        showResult('adminUsersResult', `Loaded ${formatInteger(adminUsersSnapshot.length)} users.`);
    } catch (error) {
        showResult('adminUsersResult', 'Unable to load users.', true, error.message);
    }
}

async function adminSelectUser(userId) {
    adminSelectedUserId = Number(userId);
    const selected = adminUsersSnapshot.find((u) => Number(u.user_id) === Number(adminSelectedUserId));
    if (selected) {
        adminPopulateSelectedUserForm(selected);
    } else {
        const idEl = document.getElementById('adminSelectedUserId');
        if (idEl) idEl.value = adminSelectedUserId;
    }
    adminRenderUsersList();
    await adminLoadSelectedUserAccount();
}

async function adminLoadSelectedUserAccount() {
    const userId = adminResolveSelectedUserId();
    if (!userId) {
        showResult('adminUserAccountResult', 'Select a user first.', true);
        return;
    }
    try {
        const snap = await apiRequest(`/admin/users/${userId}`);
        adminSelectedUserAccount = snap;
        adminRenderSelectedUserSnapshot();
        showResult('adminUserAccountResult', `Loaded account snapshot for user #${userId}.`);
    } catch (error) {
        showResult('adminUserAccountResult', 'Unable to load account snapshot.', true, error.message);
    }
}

async function adminUpdateSelectedUser() {
    const userId = adminResolveSelectedUserId();
    if (!userId) {
        showResult('adminUserProfileResult', 'Select a user first.', true);
        return;
    }
    const username = String(document.getElementById('adminSelectedUsername')?.value || '').trim();
    const email = String(document.getElementById('adminSelectedEmail')?.value || '').trim();
    const isAdmin = String(document.getElementById('adminSelectedIsAdmin')?.value || 'false') === 'true';
    if (!username || !email) {
        showResult('adminUserProfileResult', 'Username and email are required.', true);
        return;
    }
    try {
        await apiRequest(`/admin/users/${userId}`, {
            method: 'PATCH',
            body: JSON.stringify({
                username,
                email,
                is_admin: isAdmin
            })
        });
        showResult('adminUserProfileResult', `Updated user #${userId}.`);
        await adminLoadUsers();
        await adminLoadSelectedUserAccount();
    } catch (error) {
        showResult('adminUserProfileResult', 'Unable to update user.', true, error.message);
    }
}

async function adminResetSelectedUserPassword() {
    const userId = adminResolveSelectedUserId();
    if (!userId) {
        showResult('adminUserProfileResult', 'Select a user first.', true);
        return;
    }
    const newPassword = String(document.getElementById('adminSelectedPassword')?.value || '').trim();
    if (newPassword.length < 8) {
        showResult('adminUserProfileResult', 'Password must be at least 8 characters.', true);
        return;
    }
    try {
        await apiRequest(`/admin/users/${userId}/password`, {
            method: 'POST',
            body: JSON.stringify({ new_password: newPassword })
        });
        document.getElementById('adminSelectedPassword').value = '';
        showResult('adminUserProfileResult', `Password reset for user #${userId}.`);
    } catch (error) {
        showResult('adminUserProfileResult', 'Unable to reset password.', true, error.message);
    }
}

async function adminAdjustSelectedUserCurrency() {
    const userId = adminResolveSelectedUserId();
    if (!userId) {
        showResult('adminUserWalletAdjustResult', 'Select a user first.', true);
        return;
    }
    const currency = String(document.getElementById('adminAdjustCurrencyCode')?.value || '').trim().toUpperCase();
    const amount = String(document.getElementById('adminAdjustCurrencyAmount')?.value || '').trim();
    const reason = String(document.getElementById('adminAdjustReason')?.value || '').trim();
    if (!currency || !amount || Number(amount) === 0 || Number.isNaN(Number(amount))) {
        showResult('adminUserWalletAdjustResult', 'Enter valid signed amount and currency.', true);
        return;
    }
    try {
        await apiRequest(`/admin/users/${userId}/wallet/currency`, {
            method: 'POST',
            body: JSON.stringify({ currency, amount, reason: reason || null })
        });
        showResult('adminUserWalletAdjustResult', `Adjusted ${currency} for user #${userId}.`);
        await adminLoadSelectedUserAccount();
    } catch (error) {
        showResult('adminUserWalletAdjustResult', 'Unable to adjust currency.', true, error.message);
    }
}

async function adminAdjustSelectedUserProduct() {
    const userId = adminResolveSelectedUserId();
    if (!userId) {
        showResult('adminUserWalletAdjustResult', 'Select a user first.', true);
        return;
    }
    const product = String(document.getElementById('adminAdjustProductCode')?.value || '').trim().toUpperCase();
    const units = Math.floor(Number(document.getElementById('adminAdjustProductUnits')?.value || 0));
    const reason = String(document.getElementById('adminAdjustReason')?.value || '').trim();
    if (!product || !Number.isFinite(units) || units === 0) {
        showResult('adminUserWalletAdjustResult', 'Enter valid signed units and product.', true);
        return;
    }
    try {
        await apiRequest(`/admin/users/${userId}/wallet/product`, {
            method: 'POST',
            body: JSON.stringify({ product, amount_units: units, reason: reason || null })
        });
        showResult('adminUserWalletAdjustResult', `Adjusted ${product} for user #${userId}.`);
        await adminLoadSelectedUserAccount();
    } catch (error) {
        showResult('adminUserWalletAdjustResult', 'Unable to adjust product.', true, error.message);
    }
}

// Utility Functions
function showError(elementId, message) {
    const element = document.getElementById(elementId);
    element.textContent = message;
    element.style.display = 'block';
    setTimeout(() => {
        element.style.display = 'none';
    }, 5000);
}

function showResult(elementId, message, isError = false, detail = '') {
    const element = document.getElementById(elementId);
    element.className = `result-message ${isError ? 'error' : 'success'}`;
    element.innerHTML = `
        <div class="result-main">${message}</div>
        ${detail ? `<div class="result-detail">${detail}</div>` : ''}
    `;

    if (element.hideTimeout) {
        clearTimeout(element.hideTimeout);
    }

    element.hideTimeout = setTimeout(() => {
        element.className = 'result-message';
    }, 5000);
}

function formatDuration(totalSeconds) {
    if (totalSeconds == null || Number.isNaN(totalSeconds)) return '-';
    if (totalSeconds <= 0) return '<1s';

    const hours = Math.floor(totalSeconds / 3600);
    const minutes = Math.floor((totalSeconds % 3600) / 60);
    const seconds = totalSeconds % 60;

    if (hours > 0) {
        return `${hours}h ${minutes}m`;
    }
    if (minutes > 0) {
        return `${minutes}m ${seconds}s`;
    }
    return `${seconds}s`;
}

function formatInteger(value) {
    return integerFormatter.format(Number(value || 0));
}

function formatPrice(value) {
    const num = Number(value);
    if (!Number.isFinite(num)) return '-';
    return priceFormatter.format(num);
}

function formatSignedInteger(value) {
    const num = Number(value || 0);
    const prefix = num > 0 ? '+' : '';
    return `${prefix}${formatInteger(num)}`;
}

function formatSignedPrice(value) {
    const num = Number(value || 0);
    const prefix = num > 0 ? '+' : '';
    return `${prefix}${formatPrice(num)}`;
}

function formatSignedCompact(value) {
    const num = Number(value || 0);
    if (num === 0) return '0';
    const sign = num > 0 ? '+' : '-';
    return `${sign}${compactFormatter.format(Math.abs(num))}`;
}

function formatSignedDuration(seconds) {
    const sec = Math.trunc(Number(seconds || 0));
    if (sec === 0) return '0s';
    const prefix = sec > 0 ? '+' : '-';
    return `${prefix}${formatDuration(Math.abs(sec))}`;
}

function setSignedStat(elementId, rawValue, text) {
    const element = document.getElementById(elementId);
    if (!element) return;

    element.textContent = text;
    element.classList.remove('value-positive', 'value-negative', 'value-neutral');

    const value = Number(rawValue || 0);
    if (value > 0) {
        element.classList.add('value-positive');
    } else if (value < 0) {
        element.classList.add('value-negative');
    } else {
        element.classList.add('value-neutral');
    }
}

function formatOrderLevelExpiry(level) {
    if (level.expires_in_seconds == null) {
        return 'no expiry';
    }
    return formatDuration(Math.max(0, Number(level.expires_in_seconds) || 0));
}

function getMarketTicker(marketId) {
    return marketsById.get(marketId) || `Market #${marketId}`;
}

function renderOrderExpiry(expiresAt) {
    if (!expiresAt) return 'no expiry';
    const expiryMs = new Date(expiresAt).getTime();
    return `<span data-expiry-ts="${expiryMs}">${formatDuration(Math.ceil((expiryMs - Date.now()) / 1000))}</span>`;
}

function getEffectiveOrderState(order) {
    if (!order || !order.state) {
        return 'UNKNOWN';
    }

    if ((order.state === 'OPEN' || order.state === 'PARTIAL') && order.expires_at) {
        const expiryMs = new Date(order.expires_at).getTime();
        if (Number.isFinite(expiryMs) && expiryMs <= Date.now()) {
            return 'EXPIRED';
        }
    }

    return order.state;
}

function startExpiryTicker() {
    if (expiryTicker) {
        return;
    }

    const tick = () => {
        const expiryElements = document.querySelectorAll('[data-expiry-ts]');
        if (expiryElements.length === 0) {
            return;
        }
        expiryElements.forEach(element => {
            const expiryMs = Number(element.dataset.expiryTs);
            if (!Number.isFinite(expiryMs)) {
                element.textContent = '-';
                return;
            }
            const secondsLeft = Math.ceil((expiryMs - Date.now()) / 1000);
            element.textContent = formatDuration(secondsLeft);
        });
    };

    tick();
    expiryTicker = setInterval(tick, EXPIRY_TICK_MS);
}
