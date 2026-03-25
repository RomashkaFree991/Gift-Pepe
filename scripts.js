        window.BACKEND_URL = "https://server-gift--cizzrustex.replit.app";

// ─── Node.js API ───
        const BOT_USERNAME = 'GiftPepeRobot';
        const ADMIN_IDS = [8339935446];
        const API_BASE = String(window.BACKEND_URL || '').replace(/\/+$/, '');

        let _authUser = null;
        let _authInProgress = false;
        let _authCallbacks = [];

        function tgInitData() {
            try { return window.Telegram?.WebApp?.initData || ''; } catch (e) { return ''; }
        }

        async function apiRequest(path, options = {}) {
            const hasBody = typeof options.body !== 'undefined';
            const headers = { ...(options.headers || {}) };
            if (hasBody && !headers['Content-Type']) headers['Content-Type'] = 'application/json';

            const initData = tgInitData();
            if (initData) headers['X-Telegram-Init-Data'] = initData;

            const res = await fetch(API_BASE + path, {
                method: options.method || (hasBody ? 'POST' : 'GET'),
                headers,
                body: hasBody ? (typeof options.body === 'string' ? options.body : JSON.stringify(options.body)) : undefined
            });

            const text = await res.text();
            let json = {};
            try { json = text ? JSON.parse(text) : {}; } catch (e) {
                throw new Error(text || ('HTTP ' + res.status));
            }

            if (!res.ok) {
                const msg = json?.error?.message || json?.error || json?.message || ('HTTP ' + res.status);
                const err = new Error(msg);
                err.status = res.status;
                err.payload = json;
                throw err;
            }

            return json;
        }

        async function ensureBackendAuth() {
            if (_authUser) return _authUser;

            if (_authInProgress) {
                return new Promise(resolve => {
                    _authCallbacks.push(resolve);
                    setTimeout(() => resolve(_authUser || null), 6000);
                });
            }

            _authInProgress = true;
            try {
                const resp = await apiRequest('/api/auth/session', { body: {} });
                _authUser = resp?.user || (myUserId ? { uid: String(myUserId), telegram_id: Number(myUserId) } : null);
                return _authUser;
            } catch (e) {
                console.error('ensureBackendAuth failed', e);
                return null;
            } finally {
                _authInProgress = false;
                const cbs = _authCallbacks.splice(0);
                cbs.forEach(fn => {
                    try { fn(_authUser || null); } catch (e) {}
                });
            }
        }

        function rowIdentity(table, row) {
            if (!row) return '';
            if (row.id !== undefined && row.id !== null) return String(row.id);
            if (table === 'users') return String(row.telegram_id || '');
            if (table === 'crash_bets') return String((row.round_id || '') + ':' + (row.user_id || ''));
            if (table === 'promoCodes') return String(row.code || '');
            if (table === 'giftChecks') return String(row.code || '');
            return JSON.stringify(row);
        }

        function parseRealtimeFilters(raw) {
            const text = String(raw || '').trim();
            if (!text) return [];
            const m = text.match(/^([^=]+)=eq\.(.+)$/);
            if (!m) return [];
            const field = m[1];
            const value = /^\d+$/.test(m[2]) ? parseInt(m[2], 10) : m[2];
            return [{ type: 'eq', field, value }];
        }

        class ApiBuilder {
            constructor(table) {
                this.table = table;
                this.action = 'select';
                this.filters = [];
                this.orderCfg = null;
                this.limitN = null;
                this.fields = '*';
                this.payload = null;
                this.options = {};
                this.expect = null;
                this._promise = null;
            }

            select(fields = '*') { this.action = 'select'; this.fields = fields || '*'; return this; }
            insert(payload, options = {}) { this.action = 'insert'; this.payload = payload; this.options = options || {}; return this; }
            update(payload) { this.action = 'update'; this.payload = payload; return this; }
            delete() { this.action = 'delete'; return this; }
            upsert(payload, options = {}) { this.action = 'upsert'; this.payload = payload; this.options = options || {}; return this; }
            eq(field, value) { this.filters.push({ type: 'eq', field, value }); return this; }
            in(field, values) { this.filters.push({ type: 'in', field, values: Array.isArray(values) ? values : [] }); return this; }
            order(field, cfg = {}) { this.orderCfg = { field, ascending: cfg.ascending !== false }; return this; }
            limit(n) { this.limitN = n; return this; }
            single() { this.expect = 'single'; return this; }
            maybeSingle() { this.expect = 'maybeSingle'; return this; }

            async _exec() {
                if (this._promise) return this._promise;
                this._promise = (async () => {
                    await ensureBackendAuth();
                    try {
                        const response = await apiRequest('/api/table/' + encodeURIComponent(this.table) + '/' + this.action, {
                            body: {
                                filters: this.filters,
                                orderCfg: this.orderCfg,
                                limitN: this.limitN,
                                fields: this.fields,
                                payload: this.payload,
                                options: this.options,
                                expect: this.expect
                            }
                        });
                        return {
                            data: Object.prototype.hasOwnProperty.call(response || {}, 'data') ? response.data : null,
                            error: response?.error || null
                        };
                    } catch (error) {
                        console.error('api builder failed', this.table, this.action, error);
                        return { data: null, error };
                    }
                })();
                return this._promise;
            }

            then(resolve, reject) { return this._exec().then(resolve, reject); }
            catch(reject) { return this._exec().catch(reject); }
            finally(cb) { return this._exec().finally(cb); }
        }

        class ApiPollingChannel {
            constructor(name) {
                this.name = name;
                this.handlers = [];
                this.unsubs = [];
                this.pollMs = 1200;
            }

            on(_eventName, spec, callback) {
                this.handlers.push({ spec, callback, lastMap: new Map(), timer: null, stopped: false });
                return this;
            }

            subscribe() {
                this.unsubs = this.handlers.map(handler => {
                    const tick = async () => {
                        if (handler.stopped) return;
                        try {
                            const response = await apiRequest('/api/table/' + encodeURIComponent(handler.spec.table) + '/select', {
                                body: {
                                    filters: parseRealtimeFilters(handler.spec.filter),
                                    fields: '*'
                                }
                            });
                            const rows = Array.isArray(response?.data) ? response.data : [];
                            const nextMap = new Map();

                            rows.forEach(row => {
                                const key = rowIdentity(handler.spec.table, row);
                                nextMap.set(key, row);
                                const prev = handler.lastMap.get(key);
                                if (!prev) {
                                    handler.callback({ eventType: 'INSERT', new: row, old: null });
                                } else if (JSON.stringify(prev) !== JSON.stringify(row)) {
                                    handler.callback({ eventType: 'UPDATE', new: row, old: prev });
                                }
                            });

                            handler.lastMap.forEach((prev, key) => {
                                if (!nextMap.has(key)) {
                                    handler.callback({ eventType: 'DELETE', new: null, old: prev });
                                }
                            });

                            handler.lastMap = nextMap;
                        } catch (e) {
                            console.error('Realtime poll error', this.name, handler.spec.table, e);
                        } finally {
                            if (!handler.stopped) handler.timer = setTimeout(tick, this.pollMs);
                        }
                    };

                    tick();

                    return () => {
                        handler.stopped = true;
                        if (handler.timer) clearTimeout(handler.timer);
                    };
                });

                return this;
            }
        }

        const sb = {
            from(table) {
                return new ApiBuilder(table);
            },
            async rpc(name, args = {}) {
                await ensureBackendAuth();
                try {
                    const response = await apiRequest('/api/rpc/' + encodeURIComponent(name), { body: args || {} });
                    return {
                        data: Object.prototype.hasOwnProperty.call(response || {}, 'data') ? response.data : null,
                        error: response?.error || null
                    };
                } catch (error) {
                    console.error('rpc failed', name, error);
                    return { data: null, error };
                }
            },
            channel(name) {
                return new ApiPollingChannel(name);
            },
            removeChannel(channel) {
                try {
                    (channel?.unsubs || []).forEach(fn => { try { fn(); } catch (e) {} });
                    channel.unsubs = [];
                } catch (e) {}
            }
        };

        let myUserId = null;
        let myName = 'User';
        let myPhoto = '';

        let currentGiftCheckCode = '';
        let currentGiftCheckAmount = 0;
        let giftCheckBusy = false;
        let giftCheckToastTimer = null;
        let stickyMyGiftPreview = null;


        // ─── Prevent long-press ───
        document.addEventListener('contextmenu', e => e.preventDefault());
        document.addEventListener('dragstart', e => e.preventDefault());

        // ─── Loading Screen — держим, пока не прогрузятся баланс, топ и crash ───
        const LOADER_MIN_MS = 2800;
        const LOADER_MAX_MS = 9000;
        const loaderTasks = {
            profile: false,
            balance: false,
            top: false,
            crash: false,
            rocket: false
        };
        let _loaderTimerDone = false;
        let _loaderForceHide = false;
        function isLoaderReady() {
            return Object.values(loaderTasks).every(Boolean);
        }
        function hideLoadingScreen() {
            const loader = document.getElementById('loadingScreen');
            if (!loader || loader._hidden) return;
            if (!_loaderForceHide && !(isLoaderReady() && _loaderTimerDone)) return;
            loader._hidden = true;
            document.body.classList.add('app-ready');
            loader.classList.add('hide');
            setTimeout(() => { try { loader.remove(); } catch(e) {} }, 420);
        }
        function markLoaderTask(name) {
            if (name && Object.prototype.hasOwnProperty.call(loaderTasks, name)) {
                loaderTasks[name] = true;
            }
            hideLoadingScreen();
        }
        setTimeout(() => {
            _loaderTimerDone = true;
            hideLoadingScreen();
        }, LOADER_MIN_MS);
        setTimeout(() => {
            _loaderForceHide = true;
            hideLoadingScreen();
        }, LOADER_MAX_MS);
        async function prefetchJsonSilently(path) {
            try { await fetch(path, { cache: 'force-cache' }); } catch (e) {}
        }
        async function warmupCrashData() {
            try { await ensureBackendAuth(); } catch (e) {}
            try { await syncClock(); } catch (e) {}
            try { await loadHist(); } catch (e) {}
            try {
                const round = await getActiveRound({ ensure: true });
                if (round && isRoundStillAlive(round, 500)) {
                    currentRound = round;
                    if (!roundsSub) subRounds();
                }
            } catch (e) {}
        }

        // ─── Telegram Web App ───
        const tg = window.Telegram?.WebApp;
        if (tg) {
            tg.ready();
            tg.expand();
            tg.setHeaderColor('#1a1a2e');
            tg.setBackgroundColor('#1a1a2e');
            if (tg.requestFullscreen) tg.requestFullscreen();
            if (tg.disableVerticalSwipes) tg.disableVerticalSwipes();

            const user = tg.initDataUnsafe?.user;
            if (user) {
                myUserId = user.id;
                // Надёжный фолбэк: имя → first_name → username → 'User'
                const rawFirst = String(user.first_name || '').trim();
                const rawLast  = String(user.last_name  || '').trim();
                const rawUser  = String(user.username   || '').trim();
                myName = rawFirst
                    ? (rawLast ? rawFirst + ' ' + rawLast : rawFirst)
                    : (rawUser || 'User');
                myPhoto = user.photo_url || '';

                document.getElementById('userName').textContent = myName;
                document.getElementById('profileName').textContent = myName;
                document.getElementById('profileUsername').textContent = rawUser ? ('@' + rawUser) : '';

                const avatarEl = document.getElementById('userAvatar');
                if (myPhoto) {
                    avatarEl.src = myPhoto;
                    const pa = document.getElementById('profileAvatar');
                    pa.src = myPhoto; pa.style.display = 'block';
                } else {
                    avatarEl.style.display = 'none';
                    const ph = document.createElement('div');
                    ph.style.cssText = 'width:32px;height:32px;border-radius:50%;background:rgba(100,170,255,0.3);display:flex;align-items:center;justify-content:center;font-size:14px;font-weight:700;color:#fff;flex-shrink:0;';
                    ph.textContent = myName[0];
                    avatarEl.parentNode.insertBefore(ph, avatarEl);
                    const pp = document.getElementById('profileAvatarPlaceholder');
                    pp.textContent = myName[0]; pp.style.display = 'flex';
                }

                document.getElementById('refLinkInput').value = buildRefLink(user.id);

                const cachedBalance = readCachedBalance(user.id);
                if (cachedBalance !== null) setBalance(cachedBalance, { persist: false, cache: true });

                // Load from DB
                initDB(user).finally(async () => {
                    const warmups = [
                        refreshBalanceFromDB(user).catch(() => {}).finally(() => markLoaderTask('balance')),
                        refreshTopLeaderboard(true).catch(() => {}).finally(() => markLoaderTask('top')),
                        warmupCrashData().catch(() => {}).finally(() => markLoaderTask('crash')),
                        prefetchJsonSilently('rocket.json').finally(() => markLoaderTask('rocket'))
                    ];

                    // Если TG не дал имя/фото — пробуем из backend
                    if ((!myName || myName === 'User') || !myPhoto) {
                        try {
                            const { data } = await sb.from('users')
                                .select('first_name, photo_url, telegram_id')
                                .eq('telegram_id', user.id)
                                .limit(1)
                                .maybeSingle();
                            if (data) {
                                if ((!myName || myName === 'User') && data.first_name) {
                                    myName = data.first_name;
                                    document.getElementById('userName').textContent = myName;
                                    document.getElementById('profileName').textContent = myName;
                                }
                                if (!myPhoto && data.photo_url) {
                                    myPhoto = data.photo_url;
                                    const avatarEl = document.getElementById('userAvatar');
                                    if (avatarEl) avatarEl.src = myPhoto;
                                    const pa = document.getElementById('profileAvatar');
                                    if (pa) { pa.src = myPhoto; pa.style.display = 'block'; }
                                }
                            }
                        } catch(e) {}
                    }
                    markLoaderTask('profile');
                    await Promise.allSettled(warmups);
                    handleLaunchPayload();
                });
            }
        }


        function getLaunchPayload() {
            try {
                const direct = tg?.initDataUnsafe?.start_param;
                if (direct) return String(direct);
            } catch (e) {}
            try {
                const u = new URL(window.location.href);
                return u.searchParams.get('tgWebAppStartParam') || u.searchParams.get('startapp') || u.searchParams.get('start') || '';
            } catch (e) {
                return '';
            }
        }

        function buildRefLink(uid) {
            return 'https://t.me/' + BOT_USERNAME + '?startapp=ref_' + String(uid || '');
        }

        function parseReferrerId(payload) {
            const raw = String(payload || '').trim();
            if (!/^ref_\d+$/.test(raw)) return null;
            const refId = parseInt(raw.slice(4), 10);
            return refId > 0 ? refId : null;
        }

        async function refreshReferralStats() {
            if (!sb || !myUserId) return null;
            try {
                const { data, error } = await sb.rpc('get_referral_stats', { p_telegram_id: myUserId });
                if (error) throw error;
                const count = Math.max(0, parseInt(data?.referrals_count, 10) || 0);
                const earned = Math.max(0, parseInt(data?.earned_total, 10) || 0);
                const countEl = document.getElementById('refCountValue');
                const earnedEl = document.getElementById('refEarnedValue');
                if (countEl) countEl.textContent = count;
                if (earnedEl) earnedEl.innerHTML = earned + ' <img src="star.png" alt="star">';
                return data || null;
            } catch (e) {
                console.error('refreshReferralStats failed', e);
                return null;
            }
        }

        async function initReferralAwareProfile(user) {
            if (!sb || !user?.id) return null;
            const referrerId = parseReferrerId(getLaunchPayload());
            try {
                const { data, error } = await sb.rpc('init_user_profile', {
                    p_telegram_id: user.id,
                    p_first_name: user.first_name || '',
                    p_username: user.username || '',
                    p_photo_url: user.photo_url || '',
                    p_referrer_telegram_id: referrerId
                });
                if (error) throw error;
                if (data && typeof data.balance !== 'undefined') {
                    suppressBalanceSync = true;
                    setBalance(data.balance || 0, { persist: false, cache: true });
                    suppressBalanceSync = false;
                }
                await refreshReferralStats();
                return data || null;
            } catch (e) {
                console.error('initReferralAwareProfile failed', e);
                return null;
            }
        }

        function topPrizeForRank(rank) {
            if (rank === 1) return { name: 'Banana Pox', img: 'Gifts/Banana Pox.png' };
            if (rank === 2) return { name: 'Alien Script', img: 'Gifts/Alien Script.png' };
            if (rank === 3) return { name: 'Anatomy', img: 'Gifts/Anatomy.png' };
            return null;
        }

        function getDisplayUserName(user) {
            const first    = String(user?.first_name || '').trim();
            const last     = String(user?.last_name  || '').trim();
            const username = String(user?.username   || '').trim();
            if (first) return last ? first + ' ' + last : first;
            if (username) return username.startsWith('@') ? username : '@' + username;
            // Последний фолбэк: показываем сокращённый ID вместо «Игрок»
            const tid = user?.telegram_id || user?.id;
            if (tid) return 'User#' + String(tid).slice(-4);
            return currentLang === 'en' ? 'Player' : 'Игрок';
        }

        function renderTopLeaderboard(rows) {
            const list = document.getElementById('topList');
            const empty = document.getElementById('topEmptyState');
            if (!list) return;

            if (!Array.isArray(rows) || rows.length === 0) {
                list.innerHTML = '<div class="top-empty" id="topEmptyState">' + (currentLang === 'en' ? 'No top-up data yet' : 'Пока нет данных по пополнениям') + '</div>';
                return;
            }

            list.innerHTML = rows.map((row, idx) => {
                const rank = idx + 1;
                const prize = topPrizeForRank(rank);
                const name = getDisplayUserName(row);
                const photo = String(row?.photo_url || '').trim();
                const total = Math.max(0, parseInt(row?.total_topup, 10) || 0);
                const avatar = photo
                    ? '<img class="top-avatar" src="' + escapeHtml(photo) + '" alt="">'
                    : '<div class="top-avatar-placeholder">' + escapeHtml((name || 'U')[0].toUpperCase()) + '</div>';
                const giftHtml = prize
                    ? '<div class="top-gift" title="' + escapeHtml(prize.name) + '"><img src="' + encodeURI(prize.img) + '" alt="' + escapeHtml(prize.name) + '"></div>'
                    : '';
                return '<div class="top-row">' +
                    '<div class="top-rank top-' + rank + '">' + rank + '</div>' +
                    '<div class="top-user">' + avatar + '<div class="top-name">' + escapeHtml(name) + '</div></div>' +
                    '<div class="top-right"><div class="top-total">' + total + ' <img src="star.png" alt=""></div>' + giftHtml + '</div>' +
                    '</div>';
            }).join('');
        }

        async function refreshTopLeaderboard(force) {
            if (!sb || topLeaderboardLoading) return;
            if (!force && Array.isArray(topLeaderboardCache) && topLeaderboardCache.length && !document.getElementById('page-top')?.classList.contains('active')) return;

            topLeaderboardLoading = true;
            const list = document.getElementById('topList');
            if (list && (!Array.isArray(topLeaderboardCache) || topLeaderboardCache.length === 0)) {
                list.innerHTML = '<div class="top-empty" id="topEmptyState">' + (currentLang === 'en' ? 'Loading leaderboard...' : 'Загрузка топа...') + '</div>';
            }

            try {
                const { data, error } = await sb.rpc('get_top_toppers', { p_limit: 10 });
                if (error) throw error;
                topLeaderboardCache = Array.isArray(data) ? data : [];
                renderTopLeaderboard(topLeaderboardCache);
            } catch (e) {
                console.error('refreshTopLeaderboard failed', e);
                if (list) {
                    list.innerHTML = '<div class="top-empty" id="topEmptyState">' + (currentLang === 'en' ? 'Failed to load leaderboard' : 'Не удалось загрузить топ') + '</div>';
                }
            } finally {
                topLeaderboardLoading = false;
            }
        }

        function openCheckPage() {
            document.querySelectorAll('.tab-page').forEach(p => p.classList.remove('active'));
            const page = document.getElementById('page-check');
            if (page) page.classList.add('active');
        }

        function setGiftCheckButton(text, disabled, loading, amount, withStar = true) {
            const btn = document.getElementById('checkClaimBtn');
            if (!btn) return;
            const safeAmount = Math.max(0, parseInt(amount, 10) || 0);
            if (withStar && safeAmount > 0) {
                btn.innerHTML = '<span>' + text + ' ' + safeAmount + '</span><img src="star.png" alt="star">';
            } else {
                btn.textContent = text;
            }
            btn.disabled = !!disabled;
            btn.classList.toggle('disabled', !!disabled);
            btn.dataset.loading = loading ? '1' : '0';
        }

        function setGiftCheckSubtitle(amount, text) {
            const subtitle = document.getElementById('checkSubtitle');
            if (!subtitle) return;
            subtitle.textContent = text || t('check_subtitle');
            subtitle.dataset.i18n = text ? 'manual' : 'auto';
        }

        function renderGiftCheckState(payload = {}) {
            const amount = Math.max(0, parseInt(payload.amount, 10) || 0);
            currentGiftCheckAmount = amount;
            const hintEl = document.getElementById('checkHint');

            if (!payload.ok) {
                setGiftCheckSubtitle(amount, t('check_load_failed'));
                setGiftCheckButton(t('check_unavailable'), true, false, amount, false);
                if (hintEl) hintEl.textContent = '';
                return;
            }

            if (payload.claimed_by_current_user) {
                setGiftCheckSubtitle(amount, t('check_subtitle'));
                setGiftCheckButton(t('check_claimed'), true, false, amount, true);
                if (hintEl) hintEl.textContent = '';
                return;
            }

            if (payload.status === 'claimed') {
                setGiftCheckSubtitle(amount, t('check_subtitle'));
                setGiftCheckButton(t('check_claimed'), true, false, amount, true);
                if (hintEl) hintEl.textContent = '';
                return;
            }

            setGiftCheckSubtitle(amount, t('check_subtitle'));
            setGiftCheckButton(t('check_claim'), false, false, amount, true);
            if (hintEl) hintEl.textContent = '';
        }

        function showClaimToast(success, amount, text) {
            const toast = document.getElementById('claimToast');
            if (!toast) return;
            const icon = document.getElementById('claimToastIcon');
            const title = document.getElementById('claimToastTitle');
            const body = document.getElementById('claimToastText');
            toast.classList.remove('success', 'error', 'show');
            toast.classList.add(success ? 'success' : 'error');
            if (icon) icon.textContent = success ? '✓' : '!';
            if (title) title.textContent = success ? t('toast_success') : t('toast_error');
            if (body) body.innerHTML = (text || '') + (amount > 0 ? ' <img src="star.png" alt="star"> ' + amount : '');
            clearTimeout(giftCheckToastTimer);
            requestAnimationFrame(() => toast.classList.add('show'));
            giftCheckToastTimer = setTimeout(() => toast.classList.remove('show'), 2800);
        }

        async function loadGiftCheck(code) {
            if (!sb || !code) return;
            currentGiftCheckCode = code;
            openCheckPage();
            renderGiftCheckState({ ok: true, amount: 0, status: 'loading' });
            document.getElementById('checkHint').textContent = t('check_loading');
            try {
                const { data, error } = await sb.rpc('get_gift_check_public', {
                    p_code: code,
                    p_telegram_id: myUserId || 0
                });
                if (error) throw error;
                renderGiftCheckState(data || { ok: false, error: 'not_found' });
            } catch (e) {
                console.error('loadGiftCheck error', e);
                renderGiftCheckState({ ok: false, error: 'load_failed' });
            }
        }

        async function claimGiftCheck() {
            if (!sb || !currentGiftCheckCode || !myUserId || giftCheckBusy) return;
            giftCheckBusy = true;
            setGiftCheckButton(t('check_claiming'), true, true, currentGiftCheckAmount || 0, false);
            try {
                const { data, error } = await sb.rpc('claim_gift_check', {
                    p_code: currentGiftCheckCode,
                    p_telegram_id: myUserId,
                    p_name: myName || '',
                    p_username: (tg?.initDataUnsafe?.user?.username) || '',
                    p_photo: myPhoto || ''
                });
                if (error) throw error;

                const payload = data || {};
                if (payload.ok) {
                    if (typeof payload.balance !== 'undefined') {
                        setBalance(payload.balance, { persist: false, cache: true });
                    } else {
                        setBalance(getBalance() + (payload.amount || 0), { persist: false, cache: true });
                    }
                    renderGiftCheckState({
                        ok: true,
                        amount: payload.amount || currentGiftCheckAmount,
                        status: 'claimed',
                        claimed_by_current_user: true
                    });
                    showClaimToast(true, payload.amount || currentGiftCheckAmount, t('toast_received'));
                } else {
                    renderGiftCheckState(payload);
                    if (payload.error === 'already_claimed') {
                        showClaimToast(false, 0, t('check_claimed'));
                    } else {
                        showClaimToast(false, 0, t('toast_claim_failed'));
                    }
                }
            } catch (e) {
                console.error('claimGiftCheck error', e);
                showClaimToast(false, 0, t('toast_claim_failed'));
                renderGiftCheckState({ ok: true, amount: currentGiftCheckAmount, status: 'active' });
            } finally {
                giftCheckBusy = false;
            }
        }

        function handleLaunchPayload() {
            const payload = getLaunchPayload();
            if (payload && payload.startsWith('check_')) {
                const code = payload.slice(6).trim();
                if (code) loadGiftCheck(code);
                return;
            }
            if (payload && payload.startsWith('ref_')) {
                refreshReferralStats();
            }
        }

        // ─── DB Functions ───
        const BALANCE_CACHE_PREFIX = 'giftpepe_balance_';
        let balanceWriteTimer = null;
        let suppressBalanceSync = false;

        function balanceCacheKey(uid = myUserId) {
            return BALANCE_CACHE_PREFIX + String(uid || 'guest');
        }

        function readCachedBalance(uid = myUserId) {
            try {
                const raw = localStorage.getItem(balanceCacheKey(uid));
                if (raw === null || raw === undefined || raw === '') return null;
                return Math.max(0, parseInt(raw, 10) || 0);
            } catch (e) {
                return null;
            }
        }

        function cacheBalance(val, uid = myUserId) {
            try {
                if (!uid) return;
                localStorage.setItem(balanceCacheKey(uid), String(Math.max(0, parseInt(val, 10) || 0)));
            } catch (e) {}
        }

        async function persistBalance(_val) {
            return true;
        }

        function scheduleBalancePersist(val, _immediate) {
            cacheBalance(val);
            if (balanceWriteTimer) {
                clearTimeout(balanceWriteTimer);
                balanceWriteTimer = null;
            }
        }


        if (!tg) {
            Object.keys(loaderTasks).forEach(markLoaderTask);
            setTimeout(handleLaunchPayload, 80);
        }

        async function refreshBalanceFromDB(forceUser) {
            const uid = forceUser?.id || myUserId;
            if (!sb || !uid) return null;
            try {
                const { data, error } = await sb.from('users')
                    .select('balance')
                    .eq('telegram_id', uid)
                    .limit(1)
                    .maybeSingle();
                if (error) throw error;
                if (data) {
                    suppressBalanceSync = true;
                    setBalance(data.balance || 0, { persist: false, cache: true });
                    suppressBalanceSync = false;
                    return data.balance || 0;
                }
            } catch (e) {
                console.error('refreshBalanceFromDB failed', e);
            }
            return null;
        }


        async function initDB(user) {
            if (!user?.id) return;

            const cached = readCachedBalance(user.id);
            if (cached !== null) setBalance(cached, { persist: false, cache: true });

            if (!sb) return;
            try {
                const initData = await initReferralAwareProfile(user);

                if (!initData) {
                    const { data, error } = await sb.from('users')
                        .select('telegram_id, balance')
                        .eq('telegram_id', user.id)
                        .limit(1)
                        .maybeSingle();

                    if (error) throw error;

                    if (!data) {
                        await sb.from('users').insert({
                            telegram_id: user.id,
                            first_name: user.first_name || '',
                            username: user.username || '',
                            photo_url: user.photo_url || '',
                            balance: 0
                        });
                    } else {
                        suppressBalanceSync = true;
                        setBalance(data.balance || 0, { persist: false, cache: true });
                        suppressBalanceSync = false;

                        await sb.from('users')
                            .update({ first_name: user.first_name || '', username: user.username || '', photo_url: user.photo_url || '' })
                            .eq('telegram_id', user.id);
                    }
                }

                await refreshBalanceFromDB(user);
                await refreshReferralStats();
                await refreshInventoryFromDB(user.id);
            } catch(e) {
                console.error('DB init error', e);
            }
        }

        function getBalance() {
            return Math.max(0, parseInt(document.querySelector('.star-balance').textContent, 10) || 0);
        }

        function setBalance(val, opts = {}) {
            const safeBalance = Math.max(0, parseInt(val, 10) || 0);
            document.querySelector('.star-balance').textContent = safeBalance;
            if (opts.cache !== false) cacheBalance(safeBalance);
            if (opts.persist !== false && !suppressBalanceSync) scheduleBalancePersist(safeBalance, !!opts.immediate);
        }

        async function dbSaveInvItem(item) {
            const safeItem = sanitizeInventoryItem(item);
            if (!sb || !myUserId || !safeItem) return null;
            try {
                const { data, error } = await sb.from('inventory')
                    .insert({ user_id: myUserId, gift_name: safeItem.name, gift_img: safeItem.img, gift_price: safeItem.price })
                    .single();
                if (error) throw error;
                if (!data) return null;
                return sanitizeInventoryItem({ id: data.id, name: data.gift_name, img: data.gift_img, price: data.gift_price });
            } catch (e) {
                console.error('dbSaveInvItem failed', e);
                return null;
            }
        }

        async function dbRemoveInvItem(id) {
            if (!sb || !id || !myUserId) return false;
            try {
                const { data, error } = await sb.from('inventory')
                    .delete()
                    .eq('id', id)
                    .maybeSingle();
                if (error) throw error;
                return !!(data && data.id);
            } catch (e) {
                console.error('dbRemoveInvItem failed', e);
                return false;
            }
        }

        async function refreshInventoryFromDB(forceUserId) {
            const uid = forceUserId || myUserId;
            if (!sb || !uid) {
                inventory = inventory.map(sanitizeInventoryItem).filter(Boolean);
                renderInventory();
                return inventory;
            }
            try {
                const { data: inv, error: invErr } = await sb.from('inventory')
                    .select('id, gift_name, gift_img, gift_price, created_at')
                    .eq('user_id', uid)
                    .order('created_at', { ascending: false });
                if (invErr) throw invErr;
                inventory = (inv || []).map(d => sanitizeInventoryItem({ id: d.id, name: d.gift_name, img: d.gift_img, price: d.gift_price })).filter(Boolean);
            } catch (e) {
                console.error('refreshInventoryFromDB failed', e);
            }
            reconcileCraftSelection();
            renderInventory();
            renderCraftUI();
            renderCraftInventorySheet();
            return inventory;
        }

        document.addEventListener('visibilitychange', () => {
            if (!document.hidden) refreshBalanceFromDB();
        });

        window.addEventListener('focus', () => {
            refreshBalanceFromDB();
        });

        window.addEventListener('pagehide', () => {
            if (balanceWriteTimer) {
                clearTimeout(balanceWriteTimer);
                balanceWriteTimer = null;
            }
            if (myUserId) persistBalance(getBalance());
        });

        // ─── Navigation ───
        const navItems = document.getElementById('navItems');
        const pages = ['page-top', 'page-games', 'page-profile'];

        function positionIndicator(item) {
            // no indicator needed anymore
        }

        function switchTab(el) {
            document.querySelectorAll('.nav-item').forEach(i => i.classList.remove('active'));
            el.classList.add('active');
            // Только паузим UI-цикл, НЕ убиваем подписки и состояние раунда
            crashActive = false;
            if (gameLoop) { clearInterval(gameLoop); gameLoop = null; }
            if (pollLoop) { clearInterval(pollLoop); pollLoop = null; }
            if (roundSyncTimer) { clearTimeout(roundSyncTimer); roundSyncTimer = null; }
            if (typeof _syncRetryTimer !== 'undefined' && _syncRetryTimer) {
                clearTimeout(_syncRetryTimer); _syncRetryTimer = null;
            }
            crashRunning = false;
            const idx = parseInt(el.dataset.index);
            document.querySelectorAll('.tab-page').forEach(p => p.classList.remove('active'));
            document.getElementById(pages[idx]).classList.add('active');
            if (pages[idx] === 'page-top') {
                refreshTopLeaderboard(false);
            }
        }

        // ─── Lottie ───
        if (typeof lottie !== 'undefined') {
            lottie.loadAnimation({ container: document.getElementById('giftsLottie'), renderer: 'svg', loop: true, autoplay: true, path: 'PlushPepe%28blue%29.json' });
        }

        // ═══════════════════════════
        // ═══════════════════════════════════════
        // CRASH GAME v5 — Shared realtime round via RPC
        // ═══════════════════════════════════════
        const WAIT_MS = 10000;
        const PAUSE_MS = 3000;
        const CRASH_HOLD_MS = 2800;
        const CRASH_HIST_CACHE_KEY = 'giftpepe_crash_history_v2';
        const SPEED = 0.00018;
        const POLL_MS = 2000;
        // COUNTDOWN_SMOOTH_MS removed to fix double-10 bug

        let lastCrashHistoryRoundId = null;
        let suppressHistReloadUntil = 0;
        let crashEnterAnimTimer = null;
        let gameLoop = null, pollLoop = null;
        let crashActive = false, crashRunning = false;
        let currentRound = null, lastCountNum = -1, crashShown = false;
        let currentBet = 0, betPlaced = false, didCashOut = false, cashoutInit = false;
        let rocketAnim = null, betsSub = null, roundsSub = null;
        let roundSyncTimer = null;
        let srvOff = 0; // server - client time offset
        let renderedCrashBetKeys = new Set();
        let crashBetsState = new Map();
        let topLeaderboardLoading = false;
        let topLeaderboardCache = [];
        let roundClientAnchors = new Map();
        let pendingFreshRoundVisual = false;


        function srvNow() { return Date.now() + srvOff; }
        function mult(ms) { return Math.pow(Math.E, SPEED * ms); }
        function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }
        function roundCreatedMs(round) { return new Date(round.created_at).getTime(); }
        function trimRoundAnchors(keepKey) {
            if (roundClientAnchors.size <= 8) return;
            for (const savedKey of Array.from(roundClientAnchors.keys())) {
                if (savedKey !== keepKey) roundClientAnchors.delete(savedKey);
            }
        }
        function setRoundAnchor(round, anchor) {
            if (!round || round.id == null || !Number.isFinite(Number(anchor))) return;
            const key = String(round.id);
            const created = roundCreatedMs(round);
            roundClientAnchors.set(key, Math.max(created, Math.floor(Number(anchor))));
            trimRoundAnchors(key);
        }
        function startRoundCountdownFromNow(round) {
            if (!round || round.id == null) return;
            const created = roundCreatedMs(round);
            const now = srvNow();
            if (now >= created + WAIT_MS) return;
            setRoundAnchor(round, now);
        }
        function ensureRoundAnchor(round) {
            if (!round || round.id == null) return srvNow();
            const key = String(round.id);
            const created = roundCreatedMs(round);
            const existing = roundClientAnchors.get(key);
            if (existing !== undefined) return Math.max(created, existing);
            setRoundAnchor(round, created);
            return created;
        }
        function getActualRoundTimes(round) {
            const created = roundCreatedMs(round);
            const target = Math.max(Number(round.target_multiplier || 1.01), 1.01);
            const gameStart = created + WAIT_MS;
            const crashMs = Math.log(target) / SPEED;
            const crashAt = gameStart + crashMs;
            const nextAt = crashAt + PAUSE_MS;
            return { created, visualCreated: created, target, gameStart, crashMs, crashAt, nextAt };
        }
        function getRoundTimes(round) {
            const actual = getActualRoundTimes(round);
            const visualCreated = ensureRoundAnchor(round);
            const gameStart = visualCreated + WAIT_MS;
            const crashAt = gameStart + actual.crashMs;
            const nextAt = crashAt + PAUSE_MS;
            return { ...actual, visualCreated, gameStart, crashAt, nextAt };
        }
        function isRoundStillAlive(round, graceMs = 250) {
            return !!round && getActualRoundTimes(round).nextAt > srvNow() - graceMs;
        }
        function isFreshCountdownRound(round) {
            if (!round) return false;
            const now = srvNow();
            const times = getActualRoundTimes(round);
            const msUntilStart = times.gameStart - now;
            return msUntilStart > Math.max(1500, WAIT_MS - 1800);
        }
        async function cleanupExtraCrashRounds(rounds, keepId) {
            if (!sb || !Array.isArray(rounds) || !rounds.length || keepId === undefined || keepId === null) return;
            const now = srvNow();
            const nowIso = new Date(now).toISOString();
            const staleIds = [];
            const extraIds = [];
            for (const round of rounds) {
                if (!round || round.id === undefined || round.id === null || round.id === keepId) continue;
                const times = getActualRoundTimes(round);
                if (times.nextAt <= now - 250) staleIds.push(round.id);
                else extraIds.push(round.id);
            }
            if (staleIds.length) {
                sb.from('crash_rounds').update({ status: 'crashed', crash_time: nowIso }).in('id', staleIds).then(() => {});
            }
            if (extraIds.length) {
                sb.from('crash_rounds').update({ status: 'crashed', crash_time: nowIso }).in('id', extraIds).then(() => {});
            }
        }
        function pickCurrentCrashRound(rounds, preferredRound, opts = {}) {
            const items = Array.isArray(rounds) ? rounds.filter(Boolean).slice() : [];
            if (preferredRound && !items.some(r => String(r.id) === String(preferredRound.id))) items.push(preferredRound);
            if (!items.length) return null;
            items.sort((a, b) => roundCreatedMs(a) - roundCreatedMs(b));
            const liveRounds = items.filter(round => isRoundStillAlive(round, 250));
            if (!liveRounds.length) return null;
            if (opts.requireFreshCountdown) {
                const freshRounds = liveRounds.filter(isFreshCountdownRound);
                if (freshRounds.length) return freshRounds[freshRounds.length - 1];
                return null;
            }
            return liveRounds[liveRounds.length - 1];
        }
        function creatorDelayMs() {
            const seed = Math.abs(Number(myUserId || 0)) || Math.floor(Math.random() * 997);
            return 150 + (seed % 7) * 80;
        }
        function escapeHtml(str) {
            return String(str || '').replace(/[&<>"']/g, s => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[s]));
        }
        function crashBetKey(b) {
            if (!b) return '';
            if (b.round_id !== undefined && b.round_id !== null && b.user_id !== undefined && b.user_id !== null) {
                return String(b.round_id) + ':' + String(b.user_id);
            }
            if (b.id !== undefined && b.id !== null) return String(b.id);
            return String((b.round_id || 'round') + ':' + (b.user_id || 'user'));
        }

        // ─── Sync server clock ───
        async function syncClock() {
            if (!sb) return;
            try {
                const t1 = Date.now();
                const { data, error } = await sb.rpc('server_now');
                const t2 = Date.now();
                if (error) throw error;
                if (data) {
                    const serverT = new Date(data).getTime();
                    srvOff = serverT - ((t1 + t2) / 2);
                    return;
                }
            } catch(e) {
                console.error('server_now failed', e);
            }
            srvOff = 0;
        }

        // ─── Find or create round ───
        async function getActiveRound(opts = {}) {
            if (!sb) return null;
            const ensure = opts.ensure !== false;

            async function readActiveRows() {
                const { data, error } = await sb.from('crash_rounds')
                    .select('*')
                    .eq('status', 'active')
                    .order('created_at', { ascending: true })
                    .limit(12);
                if (error) {
                    console.error('read active round failed', error);
                    return null;
                }
                return Array.isArray(data) ? data : [];
            }

            let rows = await readActiveRows();
            if (rows === null) return null;

            let picked = pickCurrentCrashRound(rows, currentRound || null);
            if (picked) {
                cleanupExtraCrashRounds(rows, picked.id).catch(() => {});
                return picked;
            }

            if (!ensure) return null;

            const { data: rpcData, error: rpcError } = await sb.rpc('get_or_create_active_crash_round');
            if (rpcError) {
                console.error('get_or_create_active_crash_round failed', rpcError);
            }
            const rpcRound = rpcData ? (Array.isArray(rpcData) ? rpcData[0] : rpcData) : null;

            rows = await readActiveRows();
            if (rows === null) return rpcRound;

            picked = pickCurrentCrashRound(rows, rpcRound || currentRound || null);
            if (picked) {
                cleanupExtraCrashRounds(rows, picked.id).catch(() => {});
                return picked;
            }

            return rpcRound || null;
        }

        function showCrashStatus(text, isError) {
            hideAll();
            const wait = document.getElementById('crashWaitingText');
            const btn = document.getElementById('crashBetBtn');
            const players = document.getElementById('crashPlayers');
            if (players) players.innerHTML = '';
            renderedCrashBetKeys.clear();
            if (wait) {
                wait.style.display = '';
                // При загрузке — показываем «Ожидание ставок», не «Загрузка раунда»
                wait.textContent = isError ? text : 'Ожидание ставок';
                wait.style.opacity = isError ? '1' : '0.7';
            }
            if (btn) {
                if (isError) {
                    btn.innerHTML = '🔄 Повторить';
                    btn.classList.remove('placed', 'disabled');
                    btn.onclick = async () => { btn.classList.add('disabled'); await syncCrashRound(true).catch(()=>{}); };
                } else {
                    btn.innerHTML = 'Сделать ставку';
                    btn.classList.add('disabled');
                    btn.classList.remove('placed');
                    btn.onclick = null;
                }
            }
        }

        function clearCrashPlayers() {
            renderedCrashBetKeys.clear();
            crashBetsState.clear();
            const p = document.getElementById('crashPlayers');
            if (p) p.innerHTML = '';
            const t = document.getElementById('crashWaitingText');
            if (t) {
                t.style.display = '';
                t.textContent = typeof t === 'function' ? t('waiting_bets') : 'Ожидание ставок';
            }
            renderStickyGiftPreview();
        }

        function queueRoundSync(forceReloadBets) {
            if (roundSyncTimer) clearTimeout(roundSyncTimer);
            roundSyncTimer = setTimeout(() => {
                syncCrashRound(!!forceReloadBets).catch(() => {});
            }, 50);
        }

        let _syncRetryTimer = null;
        let _syncFailCount = 0;
        let crashRoundSyncBusy = false;

        async function syncCrashRound(forceReloadBets) {
            if (!crashActive || !sb) return;
            if (crashRoundSyncBusy) return;
            crashRoundSyncBusy = true;
            try {

            const now = srvNow();
            const currentTimes = currentRound ? getRoundTimes(currentRound) : null;
            const holdingCurrentRound = !!(currentTimes && now < Math.max(currentTimes.nextAt, currentTimes.crashAt + CRASH_HOLD_MS));

            if (holdingCurrentRound && !forceReloadBets) {
                return;
            }

            const round = await getActiveRound({ ensure: !holdingCurrentRound, requireFreshCountdown: pendingFreshRoundVisual && !holdingCurrentRound });

            if (!round) {
                if (holdingCurrentRound) return;

                _syncFailCount++;
                // Первые 3 попытки — тихо ретраимся, потом показываем кнопку
                if (_syncFailCount <= 3) {
                    clearTimeout(_syncRetryTimer);
                    _syncRetryTimer = setTimeout(async () => {
                        if (!crashActive) return;
                        await syncCrashRound(true).catch(() => {});
                    }, 1200 * _syncFailCount);
                    const waitEl = document.getElementById('crashWaitingText');
                    if (waitEl) { waitEl.style.display = ''; waitEl.textContent = 'Загрузка раунда...'; }
                } else {
                    _syncFailCount = 0;
                    const btn = document.getElementById('crashBetBtn');
                    const waitEl = document.getElementById('crashWaitingText');
                    if (waitEl) { waitEl.style.display = ''; waitEl.textContent = 'Нет соединения с сервером'; }
                    if (btn) {
                        btn.innerHTML = '🔄 Повторить';
                        btn.classList.remove('placed', 'disabled');
                        btn.onclick = async () => {
                            btn.classList.add('disabled');
                            btn.onclick = null;
                            btn.innerHTML = 'Загрузка...';
                            if (waitEl) waitEl.textContent = 'Загрузка раунда...';
                            await syncCrashRound(true).catch(() => {});
                        };
                    }
                }
                return;
            }

            _syncFailCount = 0;

            if (currentRound && round.id !== currentRound.id && holdingCurrentRound) {
                return;
            }

            const sameRound = !!(currentRound && currentRound.id === round.id);

            if (sameRound) {
                pendingFreshRoundVisual = false;
                currentRound = round;
                if (forceReloadBets && crashBetsState.size === 0) {
                    loadBets(round.id).catch(() => {});
                }
                if (!betsSub) subBets(round.id);
                if (!gameLoop) {
                    crashShown = false;
                    lastCountNum = -1;
                    // Если возвращаемся после паузы — восстанавливаем активную пилюлю
                    const times2 = getRoundTimes(round);
                    const n2 = srvNow();
                    if (n2 < times2.gameStart) setActiveHist('Ожидание');
                    else if (n2 < times2.crashAt) setActiveHist(mult(n2 - times2.gameStart).toFixed(2) + 'x');
                    runLoop();
                }
                return;
            }

            if (pendingFreshRoundVisual && isFreshCountdownRound(round)) {
                startRoundCountdownFromNow(round);
            }
            pendingFreshRoundVisual = false;
            currentRound = round;
            clearStickyGiftPreview();
            resetBet();
            // Сразу ставим активную пилюлю «Ожидание» в историю —
            // даже если COUNTDOWN уже прошёл, пилюля будет хотя бы на миг
            setActiveHist('Ожидание');
            subBets(round.id);
            crashShown = false;
            lastCountNum = -1;
            runLoop();
            loadBets(round.id).catch(() => {});
            } finally {
                crashRoundSyncBusy = false;
            }
        }

        function subRounds() {
            if (roundsSub && sb) { sb.removeChannel(roundsSub); roundsSub = null; }
            if (!sb) return;
            roundsSub = sb.channel('liverounds-' + Math.random().toString(36).slice(2))
                .on('postgres_changes', { event: '*', schema: 'public', table: 'crash_rounds' }, payload => {
                    const row = payload.new || payload.old;
                    if (!row) return;
                    if (row.status === 'active' || (currentRound && row.id === currentRound.id) || payload.eventType === 'INSERT') {
                        queueRoundSync(false);
                    }
                    // loadHist только когда другой клиент завершил раунд (не мы сами)
                    // Мы сами добавляем в историю через addHist() в runLoop
                    if (payload.eventType === 'UPDATE' && row.status === 'crashed') {
                        const isMine = currentRound && String(row.id) === String(currentRound.id);
                        if (!isMine && Date.now() >= suppressHistReloadUntil) loadHist().catch(() => {});
                    }
                })
                .subscribe();
        }
        let craftSelectedGifts = [];
        let craftSheetSelectedIds = [];
        let craftAnimatedGiftKeys = [];
        const CRAFT_STORAGE_PREFIX = 'giftpepe_craft_selection_';

        function craftSelectionStorageKey(uid = myUserId) {
            return uid ? (CRAFT_STORAGE_PREFIX + String(uid)) : '';
        }

        function persistCraftSelection() {
            try {
                const key = craftSelectionStorageKey();
                if (!key) return;
                const safe = craftSelectedGifts
                    .map(sanitizeInventoryItem)
                    .filter(Boolean)
                    .slice(0, 10)
                    .map(item => ({ id: item.id ?? null, name: item.name, img: item.img, price: item.price }));
                localStorage.setItem(key, JSON.stringify(safe));
            } catch (e) {}
        }

        function restoreCraftSelection(uid = myUserId) {
            try {
                const key = craftSelectionStorageKey(uid);
                if (!key) return [];
                const raw = localStorage.getItem(key);
                const parsed = raw ? JSON.parse(raw) : [];
                return Array.isArray(parsed) ? parsed.map(sanitizeInventoryItem).filter(Boolean).slice(0, 10) : [];
            } catch (e) {
                return [];
            }
        }

        function reconcileCraftSelection() {
            const saved = restoreCraftSelection();
            const invById = new Map();
            const invByPair = new Map();
            inventory.map(sanitizeInventoryItem).filter(Boolean).forEach(item => {
                if (item.id != null) invById.set(String(item.id), item);
                invByPair.set(String(item.name) + '||' + String(item.img), item);
            });
            const source = (craftSelectedGifts.length ? craftSelectedGifts : saved).map(sanitizeInventoryItem).filter(Boolean);
            const next = [];
            source.forEach(item => {
                let resolved = null;
                if (item.id != null && invById.has(String(item.id))) resolved = invById.get(String(item.id));
                else if (invByPair.has(String(item.name) + '||' + String(item.img))) resolved = invByPair.get(String(item.name) + '||' + String(item.img));
                else if (!inventory.length) resolved = item;
                if (resolved && !next.some(existing => craftGiftKey(existing) === craftGiftKey(resolved) || ((existing.name + '||' + existing.img) === (resolved.name + '||' + resolved.img)))) {
                    next.push(resolved);
                }
            });
            craftSelectedGifts = next.slice(0, 10);
            persistCraftSelection();
            return craftSelectedGifts;
        }

        function craftGiftKey(item) {
            if (!item) return '';
            return String(item.id || item.inventory_id || item.uid || '');
        }

        function craftNeedText(count) {
            const n = Math.max(0, parseInt(count, 10) || 0);
            const mod10 = n % 10;
            const mod100 = n % 100;
            let word = 'подарков';
            if (mod10 === 1 && mod100 !== 11) word = 'подарок';
            else if (mod10 >= 2 && mod10 <= 4 && (mod100 < 12 || mod100 > 14)) word = 'подарка';
            return 'Добавьте еще ' + n + ' ' + word;
        }

        function craftAddButtonText(count) {
            return count === 1 ? 'Добавить подарок' : 'Добавить подарки';
        }

        function craftCreateButtonText(count) {
            return count >= 3 ? 'Сделать контракт' : craftNeedText(3 - count);
        }

        function isGiftAlreadyInCraft(item) {
            const key = craftGiftKey(item);
            return !!key && craftSelectedGifts.some(g => craftGiftKey(g) === key);
        }

        function removeCraftGift(index) {
            craftSelectedGifts.splice(index, 1);
            persistCraftSelection();
            renderCraftUI();
            renderCraftInventorySheet();
        }

        function renderCraftUI() {
            const grid = document.getElementById('craftGrid');
            const totalEl = document.getElementById('craftTotalValue');
            const createBtn = document.getElementById('craftCreateBtn');
            if (!grid || !totalEl || !createBtn) return;

            reconcileCraftSelection();
            const animatedKeys = craftAnimatedGiftKeys.slice();
            const slots = Array.from({ length: 10 }, (_, index) => ({ item: craftSelectedGifts[index] || null, index }));
            grid.innerHTML = slots.map(({ item, index }) => {
                if (!item) return '<div class="craft-slot">Пусто</div>';
                const img = escapeHtml(String(item.img || ''));
                const key = craftGiftKey(item);
                const appearClass = animatedKeys.includes(key) ? ' craft-slot-appear' : '';
                const name = escapeHtml(String(item.name || ''));
                const price = Math.max(0, parseInt(item.price, 10) || 0);
                return '<button class="craft-slot filled' + appearClass + '" type="button" onclick="removeCraftGift(' + index + ')">' +
                    '<img class="craft-slot-img" src="' + img + '" alt="">' +
                    '<div class="craft-slot-name">' + name + '</div>' +
                    '<div class="craft-slot-price"><img src="star.png" alt="">' + price + '</div>' +
                '</button>';
            }).join('');

            const total = craftSelectedGifts.reduce((sum, item) => sum + (Math.max(0, parseInt(item && item.price, 10) || 0)), 0);
            totalEl.textContent = String(total);

            const enough = craftSelectedGifts.length >= 3;
            createBtn.textContent = craftCreateButtonText(craftSelectedGifts.length);
            createBtn.disabled = !enough;
            createBtn.classList.toggle('ready', enough);

            if (animatedKeys.length) {
                clearTimeout(window.__craftAppearTimer);
                window.__craftAppearTimer = setTimeout(() => {
                    craftAnimatedGiftKeys = [];
                }, 420);
            }
            persistCraftSelection();
        }

        function openCraftInventorySheet() {
            renderCraftInventorySheet();
            document.getElementById('craftSheetOverlay')?.classList.add('show');
            document.getElementById('craftSheet')?.classList.add('show');
        }

        function closeCraftInventorySheet() {
            document.getElementById('craftSheetOverlay')?.classList.remove('show');
            document.getElementById('craftSheet')?.classList.remove('show');
            craftSheetSelectedIds = [];
            updateCraftSheetAddButton();
            renderCraftInventorySheet();
        }

        function toggleCraftInventorySelection(index) {
            const item = sanitizeInventoryItem(inventory[index]);
            if (!item || !item.id) return;
            if (isGiftAlreadyInCraft(item)) return;
            const key = craftGiftKey(item);
            if (!key) return;
            if (craftSheetSelectedIds.includes(key)) craftSheetSelectedIds = craftSheetSelectedIds.filter(id => id !== key);
            else {
                const freeSlots = Math.max(0, 10 - craftSelectedGifts.length);
                if (craftSheetSelectedIds.length >= freeSlots) return;
                craftSheetSelectedIds.push(key);
            }
            updateCraftSheetAddButton();
            renderCraftInventorySheet();
        }

        function updateCraftSheetAddButton() {
            const btn = document.getElementById('craftSheetAddBtn');
            if (!btn) return;
            const count = craftSheetSelectedIds.length;
            btn.textContent = craftAddButtonText(count || 2);
            btn.classList.toggle('show', count > 0);
        }

        function ensureCraftEmptyLottie() {
            const holder = document.getElementById('craftEmptyLottie');
            if (!holder || holder.dataset.ready === '1' || typeof lottie === 'undefined') return;
            holder.dataset.ready = '1';
            lottie.loadAnimation({
                container: holder,
                renderer: 'svg',
                loop: true,
                autoplay: true,
                path: 'PlushPepe%28blue%29.json'
            });
        }

        function renderCraftInventorySheet() {
            const grid = document.getElementById('craftInventoryGrid');
            if (!grid) return;
            reconcileCraftSelection();
            const items = inventory.map(sanitizeInventoryItem).filter(Boolean);
            grid.classList.toggle('is-empty', !items.length);
            if (!items.length) {
                grid.innerHTML = '<div class="craft-sheet-empty">' +
                    '<div class="craft-sheet-empty-lottie" id="craftEmptyLottie"></div>' +
                    '<div class="craft-sheet-empty-title">Не нашли подарков?</div>' +
                    '<div class="craft-sheet-empty-text">Пополните в @GiftPepeSupport или выиграйте в играх.</div>' +
                '</div>';
                ensureCraftEmptyLottie();
                updateCraftSheetAddButton();
                return;
            }
            grid.innerHTML = items.map((item, index) => {
                const key = craftGiftKey(item);
                const inCraft = isGiftAlreadyInCraft(item);
                const selected = craftSheetSelectedIds.includes(key);
                const cls = 'craft-sheet-item' + (selected ? ' selected' : '') + (inCraft ? ' in-craft' : '');
                const badge = inCraft ? '<div class="craft-sheet-item-badge">В крафте</div>' : (selected ? '<div class="craft-sheet-item-badge">Выбрано</div>' : '');
                return '<button type="button" class="' + cls + '" onclick="toggleCraftInventorySelection(' + index + ')" ' + (inCraft ? 'disabled' : '') + '>' +
                    badge +
                    '<img src="' + escapeHtml(String(item.img || '')) + '" alt="">' +
                    '<div class="craft-sheet-item-name">' + escapeHtml(String(item.name || '')) + '</div>' +
                    '<div class="craft-sheet-item-price"><img src="star.png" alt="">' + Math.max(0, parseInt(item.price, 10) || 0) + '</div>' +
                '</button>';
            }).join('');
            updateCraftSheetAddButton();
        }

        function addSelectedCraftGifts() {
            if (!craftSheetSelectedIds.length) return;
            const freeSlots = Math.max(0, 10 - craftSelectedGifts.length);
            if (!freeSlots) {
                closeCraftInventorySheet();
                return;
            }
            const selectedItems = inventory
                .map(sanitizeInventoryItem)
                .filter(Boolean)
                .filter(item => craftSheetSelectedIds.includes(craftGiftKey(item)))
                .slice(0, freeSlots);
            craftAnimatedGiftKeys = [];
            selectedItems.forEach(item => {
                if (!isGiftAlreadyInCraft(item)) {
                    craftSelectedGifts.push(item);
                    const key = craftGiftKey(item);
                    if (key) craftAnimatedGiftKeys.push(key);
                }
            });
            persistCraftSelection();
            closeCraftInventorySheet();
            renderCraftUI();
        }

        function openCraft() {
            document.querySelectorAll('.tab-page').forEach(p => p.classList.remove('active'));
            document.getElementById('page-craft').classList.add('active');
            renderCraftUI();
        }

        function closeCraft() {
            closeCraftInventorySheet();
            document.getElementById('page-craft').classList.remove('active');
            document.getElementById('page-games').classList.add('active');
        }

        function animateCrashEntrance() {
            const page = document.getElementById('page-crash');
            if (!page) return;
            page.classList.remove('entering');
            void page.offsetWidth;
            page.classList.add('entering');
            clearTimeout(crashEnterAnimTimer);
            crashEnterAnimTimer = setTimeout(() => page.classList.remove('entering'), 420);
        }

        document.getElementById('craftAddBtn')?.addEventListener('click', openCraftInventorySheet);
        document.getElementById('craftSheetAddBtn')?.addEventListener('click', addSelectedCraftGifts);

        // ─── Open / Close ───
        function openGame(name) {
            if (name === 'craft') {
                openCraft();
                return;
            }

            if (name === 'crash') {
                document.querySelectorAll('.tab-page').forEach(p => p.classList.remove('active'));
                document.getElementById('page-crash').classList.add('active');
                animateCrashEntrance();
                crashActive = true;
                genStars();

                // Если раунд уже загружен и живой — мгновенно возобновляем без сетевых запросов
                const roundAlive = currentRound && isRoundStillAlive(currentRound, 500);
                if (roundAlive && (betsSub || roundsSub)) {
                    // Просто перезапускаем UI-цикл и poll — без syncClock/loadHist/loadBets
                    if (!gameLoop) {
                        crashShown = false;
                        lastCountNum = -1;
                        runLoop();
                    }
                    if (!pollLoop) {
                        pollLoop = setInterval(async () => {
                            if (!crashActive) return;
                            try {
                                if (!gameLoop && !roundSyncTimer) await syncCrashRound(true);
                                else await syncCrashRound(false);
                            } catch(e) {}
                        }, POLL_MS);
                    }
                    return;
                }

                // Первый вход или раунд устарел — полная инициализация
                stopAll();
                initCrash();
            }
        }

        function closeCrash() {
            crashActive = false;
            clearStickyGiftPreview();
            // Только паузим UI-цикл — подписки и currentRound остаются живыми
            if (gameLoop) { clearInterval(gameLoop); gameLoop = null; }
            if (pollLoop) { clearInterval(pollLoop); pollLoop = null; }
            if (roundSyncTimer) { clearTimeout(roundSyncTimer); roundSyncTimer = null; }
            if (_syncRetryTimer) { clearTimeout(_syncRetryTimer); _syncRetryTimer = null; }
            crashRunning = false;
            hideAll();
            document.getElementById('page-crash').classList.remove('active');
            document.getElementById('page-games').classList.add('active');
        }

        function stopAll() {
            if (gameLoop) { clearInterval(gameLoop); gameLoop = null; }
            if (pollLoop) { clearInterval(pollLoop); pollLoop = null; }
            if (roundSyncTimer) { clearTimeout(roundSyncTimer); roundSyncTimer = null; }
            if (_syncRetryTimer) { clearTimeout(_syncRetryTimer); _syncRetryTimer = null; }
            if (betsSub && sb) { sb.removeChannel(betsSub); betsSub = null; }
            if (roundsSub && sb) { sb.removeChannel(roundsSub); roundsSub = null; }
            _syncFailCount = 0;
            crashRunning = false;
            hideAll();
        }

        function hideAll() {
            const m = document.getElementById('crashMultiplier');
            if (m) { m.style.display = 'none'; m.classList.remove('crashed'); }
            const r = document.getElementById('crashRocket');
            if (r) r.style.display = 'none';
            const c = document.getElementById('crashCountdown');
            if (c) c.style.display = 'none';
        }

        function resetBet() {
            betPlaced = false; currentBet = 0; didCashOut = false; cashoutInit = false;
            const btn = document.getElementById('crashBetBtn');
            if (btn) { btn.innerHTML = 'Сделать ставку'; btn.classList.remove('placed','disabled'); btn.onclick = function(){ placeBet(); }; }
            clearCrashPlayers();
            resetGiftDisplay();
        }

        // ─── Stars ───
        function genStars() {
            const c = document.getElementById('crashStars');
            if (c.querySelectorAll('.crash-star').length > 0) return;
            for (let i = 0; i < 100; i++) {
                const s = document.createElement('div'); s.className = 'crash-star';
                s.style.left = Math.random()*100+'%'; s.style.top = Math.random()*100+'%';
                s.style.opacity = Math.random()*0.5+0.15;
                const z = Math.random()*2.5+0.5;
                s.style.width = z+'px'; s.style.height = z+'px';
                c.appendChild(s);
            }
        }

        // ─── History UI ───
        function getCrashHistoryTextsFromDom() {
            const h = document.getElementById('crashHistory');
            if (!h) return [];
            return Array.from(h.querySelectorAll('.crash-history-item.past'))
                .map(el => String(el.textContent || '').trim())
                .filter(Boolean)
                .slice(0, 8);
        }

        function saveCrashHistoryCache(texts) {
            try {
                const safe = (Array.isArray(texts) ? texts : [])
                    .map(v => String(v || '').trim())
                    .filter(Boolean)
                    .slice(0, 8);
                localStorage.setItem(CRASH_HIST_CACHE_KEY, JSON.stringify(safe));
            } catch (e) {}
        }

        function getCrashHistoryCache() {
            try {
                const raw = localStorage.getItem(CRASH_HIST_CACHE_KEY);
                const parsed = raw ? JSON.parse(raw) : [];
                return Array.isArray(parsed)
                    ? parsed.map(v => String(v || '').trim()).filter(Boolean).slice(0, 8)
                    : [];
            } catch (e) {
                return [];
            }
        }

        function renderCrashHistoryPast(texts) {
            const h = document.getElementById('crashHistory');
            if (!h) return;

            const safeTexts = (Array.isArray(texts) ? texts : [])
                .map(v => String(v || '').trim())
                .filter(Boolean)
                .slice(0, 8);

            const existing = getCrashHistoryTextsFromDom();
            let active = h.querySelector('.crash-history-item.active');

            if (JSON.stringify(existing) === JSON.stringify(safeTexts)) {
                if (active && h.firstChild !== active) h.insertBefore(active, h.firstChild);
                return;
            }

            Array.from(h.querySelectorAll('.crash-history-item.past')).forEach(el => el.remove());

            if (active && h.firstChild !== active) h.insertBefore(active, h.firstChild);

            const anchor = active ? active.nextSibling : null;
            safeTexts.forEach(text => {
                const e = document.createElement('div');
                e.className = 'crash-history-item past';
                e.textContent = text;
                h.insertBefore(e, anchor);
            });

            saveCrashHistoryCache(safeTexts);
        }

        function addHist(text, cls) {
            if (cls !== 'past') return;
            const safeText = String(text || '').trim();
            if (!safeText) return;

            const next = [safeText, ...getCrashHistoryTextsFromDom().filter(item => item !== safeText)].slice(0, 8);
            renderCrashHistoryPast(next);

            const h = document.getElementById('crashHistory');
            const firstPast = h ? h.querySelector('.crash-history-item.past') : null;
            if (firstPast) {
                firstPast.classList.add('new-entry');
                firstPast.addEventListener('animationend', () => firstPast.classList.remove('new-entry'), { once: true });
            }
        }

        function setActiveHist(text) {
            const h = document.getElementById('crashHistory');
            if (!h) return;
            let a = h.querySelector('.crash-history-item.active');
            if (!a) {
                a = document.createElement('div');
                a.className = 'crash-history-item active';
            }
            a.textContent = text;
            if (h.firstChild !== a) h.insertBefore(a, h.firstChild);
        }

        function rmActiveHist() {
            const a = document.querySelector('.crash-history-item.active');
            if (a) a.remove();
        }

        async function loadHist(force = false) {
            if (!sb) return;
            if (!force && Date.now() < suppressHistReloadUntil) return;
            const { data, error } = await sb.from('crash_rounds')
                .select('id,target_multiplier')
                .eq('status', 'crashed')
                .order('created_at', { ascending: false })
                .limit(8);

            if (error) {
                console.error('loadHist failed', error);
                return;
            }

            const texts = Array.isArray(data)
                ? data.map(r => Number(r.target_multiplier || 1).toFixed(2) + 'x')
                : [];

            renderCrashHistoryPast(texts);
        }

        function deriveGiftForAmount(amount) {
            const safeAmount = Math.max(0, Math.floor(Number(amount) || 0));
            for (let i = giftList.length - 1; i >= 0; i--) {
                if (safeAmount >= giftList[i].price) return giftList[i];
            }
            return null;
        }

        function trackMyPotentialGift(amount) {
            const safeAmount = Math.max(0, Math.floor(Number(amount) || 0));
            let idx = -1;
            for (let i = giftList.length - 1; i >= 0; i--) {
                if (safeAmount >= giftList[i].price) {
                    idx = i;
                    break;
                }
            }
            currentGiftIdx = idx;
            wonGift = idx >= 0 ? giftList[idx] : null;
            return wonGift;
        }

        function setPlayerGift(row, amount, status) {
            if (!row) return null;
            const giftEl = row.querySelector('.cp-gift');
            if (!giftEl) return null;

            if (status === 'lost') {
                giftEl.style.display = 'none';
                giftEl.removeAttribute('src');
                giftEl.classList.remove('loading');
                return null;
            }

            const gift = deriveGiftForAmount(amount);
            if (gift) {
                const nextSrc = encodeURI(gift.img);
                if (giftEl.getAttribute('src') !== nextSrc) {
                    giftEl.classList.add('loading');
                    giftEl.onload = function() { this.classList.remove('loading'); };
                    giftEl.onerror = function() { this.classList.add('loading'); };
                    giftEl.src = nextSrc;
                }
                giftEl.style.display = 'block';
            } else {
                giftEl.style.display = 'none';
                giftEl.removeAttribute('src');
                giftEl.classList.remove('loading');
            }
            return gift;
        }

        function setPlayerRowStatus(row, status) {
            if (!row) return;
            row.classList.remove('status-active', 'status-won', 'status-lost');
            if (status === 'won') row.classList.add('status-won');
            else if (status === 'lost') row.classList.add('status-lost');
            else row.classList.add('status-active');
        }

        function normalizeBetRecord(b) {
            const record = {
                key: crashBetKey(b),
                round_id: b.round_id,
                user_id: b.user_id,
                user_name: b.user_name || '',
                user_photo: b.user_photo || '',
                amount: Math.max(0, Math.floor(Number(b.amount) || 0)),
                status: (b.status || 'active').toLowerCase(),
                cashout_amount: b.cashout_amount == null ? null : Math.max(0, Math.floor(Number(b.cashout_amount) || 0)),
                cashout_multiplier: b.cashout_multiplier == null ? null : Number(b.cashout_multiplier),
                reward_claimed: !!b.reward_claimed,
                reward_type: b.reward_type || null,
                created_at: b.created_at,
                isMe: Number(b.user_id) === Number(myUserId)
            };
            if (record.status !== 'won' && record.status !== 'lost') record.status = 'active';
            return record;
        }

        function upsertCrashPlayerRow({ key, name, photo, amount, currentAmount, isMe, status }) {
            const safeKey = escapeHtml(key);
            const players = document.getElementById('crashPlayers');
            const wait = document.getElementById('crashWaitingText');
            if (!players) return null;
            if (wait) wait.style.display = 'none';

            const displayName = isMe ? 'Вы' : (name || 'Player');
            const shownAmount = Math.max(0, Math.floor(Number(currentAmount != null ? currentAmount : amount) || 0));

            let row = players.querySelector('[data-bet-key="' + safeKey + '"]');

            if (!row) {
                // Создаём новую строку
                const avatar = photo
                    ? '<img class="cp-avatar" src="' + escapeHtml(photo) + '" alt="">'
                    : '<div class="cp-avatar-placeholder">' + escapeHtml((displayName || 'P')[0]) + '</div>';
                const markup = '<div class="crash-player-row' + (isMe ? ' me' : '') + '" data-bet-key="' + safeKey + '">' +
                    avatar +
                    '<div class="cp-name"><span class="cp-name-text">' + escapeHtml(displayName) + '</span><span class="cp-original-bet">' + amount + ' <img src="star.png" alt=""></span></div>' +
                    '<span class="cp-amount"><span class="cp-growing-amount">' + shownAmount + '</span> <img src="star.png" alt=""></span>' +
                    '<img class="cp-gift" src="" alt="" style="display:none;">' +
                    '</div>';
                const wrap = document.createElement('div');
                wrap.innerHTML = markup;
                row = wrap.firstChild;
                if (isMe) players.prepend(row); else players.appendChild(row);
            } else {
                // Обновляем только текст/числа — НЕ заменяем весь элемент,
                // чтобы не сбросить отображение подарка и не плодить дублей
                const growEl = row.querySelector('.cp-growing-amount');
                if (growEl) growEl.textContent = shownAmount;
            }

            setPlayerRowStatus(row, status || 'active');
            renderedCrashBetKeys.add(String(key));
            return row;
        }

        function syncMyBetStateFromRecord(rec) {
            if (!rec.isMe) return;
            currentBet = rec.amount;

            if (rec.status === 'won') {
                didCashOut = true;
                betPlaced = false;
                cashoutInit = false;
                setBtnState('Забрали ' + (rec.cashout_amount || rec.amount) + ' <img src="star.png" alt="">', 'placed');
            } else if (rec.status === 'lost') {
                betPlaced = false;
                didCashOut = false;
                cashoutInit = false;
            } else {
                betPlaced = true;
                didCashOut = false;
                cashoutInit = false;
                setBtnState('Ставка сделана', 'placed');
            }
        }

        function renderBetRecord(rec) {
            const shownAmount = rec.status === 'won' && rec.cashout_amount ? rec.cashout_amount : rec.amount;
            const shouldShowStickyGiftOnly = !!(
                rec &&
                rec.isMe &&
                rec.status === 'won' &&
                stickyMyGiftPreview &&
                (rec.reward_claimed || rec.reward_type === 'gift' || rec.reward_type === 'gift_pending')
            );

            if (shouldShowStickyGiftOnly) {
                const existingRow = document.querySelector('[data-bet-key="' + escapeHtml(rec.key) + '"]');
                if (existingRow) existingRow.remove();
                renderStickyGiftPreview();
                trackMyPotentialGift(rec.cashout_amount || rec.amount);
                syncMyBetStateFromRecord(rec);
                return;
            }

            const row = upsertCrashPlayerRow({
                key: rec.key,
                name: rec.isMe ? myName : (rec.user_name || 'Player'),
                photo: rec.isMe ? myPhoto : (rec.user_photo || ''),
                amount: rec.amount,
                currentAmount: shownAmount,
                isMe: rec.isMe,
                status: rec.status
            });

            if (rec.status === 'won') {
                setPlayerGift(row, rec.cashout_amount || rec.amount, rec.status);
            } else if (rec.status === 'active') {
                setPlayerGift(row, shownAmount, rec.status);
            } else {
                setPlayerGift(row, 0, rec.status);
            }

            if (rec.isMe) {
                if (rec.status === 'won') trackMyPotentialGift(rec.cashout_amount || rec.amount);
                else if (rec.status === 'active') trackMyPotentialGift(shownAmount);
                else trackMyPotentialGift(0);
                syncMyBetStateFromRecord(rec);
            }
        }

        function applyBetPayload(payloadBet) {
            const rec = normalizeBetRecord(payloadBet);
            crashBetsState.set(rec.key, rec);
            renderBetRecord(rec);
        }

        function markAllActiveBetsAsLostLocal() {
            crashBetsState.forEach((rec, key) => {
                if (rec.status === 'active') {
                    rec.status = 'lost';
                    crashBetsState.set(key, rec);
                    renderBetRecord(rec);
                }
            });
            trackMyPotentialGift(0);
        }

        function syncLiveBetAmounts(multiplier) {
            crashBetsState.forEach(rec => {
                const row = document.querySelector('[data-bet-key="' + escapeHtml(rec.key) + '"]');
                if (!row) return;

                if (rec.status === 'active') {
                    const liveAmount = Math.max(0, Math.floor(rec.amount * multiplier));
                    const amountEl = row.querySelector('.cp-growing-amount');
                    if (amountEl) amountEl.textContent = liveAmount;
                    setPlayerGift(row, liveAmount, rec.status);
                    if (rec.isMe) trackMyPotentialGift(liveAmount);
                } else if (rec.status === 'won') {
                    const frozen = rec.cashout_amount || rec.amount;
                    const amountEl = row.querySelector('.cp-growing-amount');
                    if (amountEl) amountEl.textContent = frozen;
                    setPlayerGift(row, frozen, rec.status);
                    if (rec.isMe) trackMyPotentialGift(frozen);
                } else {
                    const amountEl = row.querySelector('.cp-growing-amount');
                    if (amountEl) amountEl.textContent = rec.amount;
                    setPlayerGift(row, 0, rec.status);
                }
            });
        }

        async function loadBets(rid) {
            if (!sb) return;
            const optimisticMyBet = myUserId ? crashBetsState.get(crashBetKey({ round_id: rid, user_id: myUserId })) : null;
            clearCrashPlayers();
            const { data, error } = await sb.from('crash_bets')
                .select('*')
                .eq('round_id', rid)
                .order('created_at', { ascending: true });

            if (error) {
                console.error('loadBets failed', error);
                if (optimisticMyBet && optimisticMyBet.status === 'active') applyBetPayload(optimisticMyBet);
                return;
            }

            const seenKeys = new Set();
            if (data && data.length) {
                data.forEach(item => {
                    seenKeys.add(crashBetKey(item));
                    applyBetPayload(item);
                });
            }
            if (optimisticMyBet && optimisticMyBet.status === 'active' && !seenKeys.has(String(optimisticMyBet.key || ''))) {
                applyBetPayload(optimisticMyBet);
            }
        }

        function setBtnState(html, cls) {
            const btn = document.getElementById('crashBetBtn');
            btn.innerHTML = html;
            btn.classList.remove('placed','disabled');
            if (cls) btn.classList.add(cls);
            btn.onclick = null;
        }

        function subBets(rid) {
            if (betsSub && sb) {
                sb.removeChannel(betsSub);
                betsSub = null;
            }
            if (!sb) return;

            betsSub = sb.channel('livebets-' + rid + '-' + Math.random().toString(36).slice(2))
                .on('postgres_changes', { event: '*', schema: 'public', table: 'crash_bets', filter: 'round_id=eq.' + rid }, payload => {
                    const b = payload.new || payload.old;
                    if (!b) return;

                    const key = crashBetKey(b);
                    if (payload.eventType === 'DELETE') {
                        const row = document.querySelector('[data-bet-key="' + escapeHtml(key) + '"]');
                        if (row) row.remove();
                        renderedCrashBetKeys.delete(String(key));
                        crashBetsState.delete(String(key));
                        return;
                    }

                    // INSERT — пропускаем если ставка уже отрисована локально
                    // (showMyBet или loadBets уже добавили её)
                    if (payload.eventType === 'INSERT') {
                        if (crashBetsState.has(String(key)) || renderedCrashBetKeys.has(String(key))) {
                            // Всё равно обновляем state с серверными данными (статус мог смениться)
                            const existing = crashBetsState.get(String(key));
                            const incoming = normalizeBetRecord(b);
                            if (existing && incoming.status !== 'active') {
                                // Статус изменился — перерисовываем
                                applyBetPayload(b);
                            }
                            return;
                        }
                    }

                    applyBetPayload(b);
                })
                .subscribe();
        }

        function showMyBet(amt) {
            const rec = {
                round_id: currentRound ? currentRound.id : 'round',
                user_id: myUserId || 'guest',
                user_name: myName,
                user_photo: myPhoto,
                amount: amt,
                status: 'active'
            };
            applyBetPayload(rec);
        }

        function showOtherBet(name, photo, amt, key) {
            applyBetPayload({
                round_id: currentRound ? currentRound.id : 'round',
                user_id: key || (name + ':' + photo + ':' + amt),
                user_name: name,
                user_photo: photo,
                amount: amt,
                status: 'active'
            });
        }

        // ═══════════════════════════════════════
        // MAIN INIT + GAME LOOP
        // ═══════════════════════════════════════
        async function initCrash() {
            if (!crashActive) return;
            hideAll();
            resetBet();

            const cachedHist = getCrashHistoryCache();
            if (cachedHist.length) renderCrashHistoryPast(cachedHist);
            if (!currentRound) setActiveHist('Ожидание');

            const btn = document.getElementById('crashBetBtn');
            // Показываем "Подключение..." только если авторизации ещё нет
            const alreadyAuthed = !!_authUser;
            if (!alreadyAuthed && btn) { btn.innerHTML = 'Подключение...'; btn.classList.add('disabled'); btn.onclick = null; }

            const user = await ensureBackendAuth();
            if (!crashActive) return; // пользователь закрыл экран пока ждали

            if (!user) {
                const errBox = document.getElementById('crashWaitingText');
                if (errBox) errBox.style.display = '';
                if (btn) {
                    btn.innerHTML = '🔄 Повторить';
                    btn.classList.remove('placed');
                    btn.classList.add('disabled');
                    btn.onclick = () => { btn.classList.add('disabled'); btn.onclick = null; initCrash(); };
                    btn.classList.remove('disabled');
                }
                const waitEl = document.getElementById('crashWaitingText');
                if (waitEl) waitEl.textContent = '⛔ Backend недоступен';
                console.error('Backend недоступен');
                return;
            }

            // Не тормозим вход в игру ожиданием истории/часов — догружаем это в фоне.
            const now5 = Date.now();
            if (!window._lastClockSync || now5 - window._lastClockSync > 5 * 60 * 1000) {
                syncClock()
                    .then(() => { window._lastClockSync = Date.now(); })
                    .catch(() => {});
            }

            if (!roundsSub) subRounds();
            loadHist().catch(() => {});
            await syncCrashRound(true);

            // Poll для резервной синхронизации
            pollLoop = setInterval(async () => {
                if (!crashActive) return;
                try {
                    if (!gameLoop && !roundSyncTimer) {
                        await syncCrashRound(true);
                    } else {
                        await syncCrashRound(false);
                    }
                } catch(e) {}
            }, POLL_MS);
        }

        function runLoop() {
            if (gameLoop) clearInterval(gameLoop);
            let rocketOn = false;

            const { gameStart, target, crashAt, nextAt } = getRoundTimes(currentRound);

            gameLoop = setInterval(async () => {
                if (!crashActive) { clearInterval(gameLoop); return; }
                const now = srvNow();
                const countEl = document.getElementById('crashCountdown');
                const multiEl = document.getElementById('crashMultiplier');
                const rocketEl = document.getElementById('crashRocket');

                // ── COUNTDOWN ──
                if (now < gameStart) {
                    multiEl.style.display = 'none';
                    multiEl.classList.remove('crashed');
                    rocketEl.style.display = 'none';
                    countEl.style.display = 'block';

                    const msLeft = Math.max(0, gameStart - now);
                    const sec = Math.max(1, Math.min(Math.ceil(msLeft / 1000), 10));

                    if (sec !== lastCountNum) {
                        const firstShow = lastCountNum < 0;
                        lastCountNum = sec;
                        countEl.textContent = sec;
                        if (firstShow) {
                            // Первый показ после входа/возврата — без анимации, сразу видно
                            countEl.classList.remove('pop');
                            countEl.style.cssText = 'display:block;opacity:1;transform:translateY(-50%) scale(1);';
                        } else {
                            countEl.style.cssText = '';
                            countEl.classList.remove('pop');
                            void countEl.offsetWidth;
                            countEl.classList.add('pop');
                        }
                    }

                    setActiveHist('Ожидание');
                    crashRunning = false;
                    return;
                }

                // ── FLYING ──
                if (now >= gameStart && now < crashAt) {
                    countEl.style.display = 'none';
                    multiEl.style.display = 'none';
                    multiEl.classList.remove('crashed');

                    if (!rocketOn) {
                        rocketOn = true;
                        clearStickyGiftPreview();
                        crashRunning = true;
                        closeBetMenu();
                        rocketEl.style.display = 'block';

                        if (!rocketAnim && typeof lottie !== 'undefined') {
                            rocketAnim = lottie.loadAnimation({ container: rocketEl, renderer: 'svg', loop: true, autoplay: true, path: 'rocket.json' });
                        } else if (rocketAnim) {
                            rocketAnim.goToAndPlay(0);
                        }

                        if (!betPlaced && !didCashOut) {
                            const btn = document.getElementById('crashBetBtn');
                            btn.innerHTML = 'Сделать ставку';
                            btn.classList.add('disabled');
                            btn.onclick = null;
                        }
                    }

                    const m = mult(now - gameStart);
                    setActiveHist(m.toFixed(2) + 'x');
                    syncLiveBetAmounts(m);

                    if (betPlaced && !didCashOut) {
                        updateCash(m);
                    }
                    return;
                }

                // ── CRASHED ──
                if (now >= crashAt && now < nextAt) {
                    if (!crashShown) {
                        crashShown = true;
                        crashRunning = false;
                        countEl.style.display = 'none';
                        rocketEl.style.display = 'none';
                        if (rocketAnim) rocketAnim.stop();

                        multiEl.textContent = Number(target).toFixed(2) + 'x';
                        multiEl.style.display = 'block';
                        multiEl.classList.add('crashed');

                        setBtnState('Краш ' + Number(target).toFixed(2) + 'x', 'disabled');
                        rmActiveHist();

                        if (betPlaced && sb) {
                            sb.from('crash_bets').update({ status:'lost' }).eq('round_id', currentRound.id).eq('user_id', myUserId).then(()=>{});
                        }

                        markAllActiveBetsAsLostLocal();
                        if (lastCrashHistoryRoundId !== String(currentRound && currentRound.id)) {
                            addHist(Number(target).toFixed(2) + 'x', 'past');
                            lastCrashHistoryRoundId = String(currentRound && currentRound.id);
                        }
                        suppressHistReloadUntil = Date.now() + 1400;
                        betPlaced = false;
                        didCashOut = false;
                        resetGiftDisplay();

                        if (sb) {
                            sb.from('crash_rounds').update({ status:'crashed', crash_time: new Date(now).toISOString() }).eq('id', currentRound.id).eq('status','active').then(()=>{});
                        }
                    }
                    return;
                }

                // ── NEXT ROUND ──
                if (now >= nextAt) {
                    clearInterval(gameLoop);
                    gameLoop = null;

                    if (betsSub && sb) {
                        sb.removeChannel(betsSub);
                        betsSub = null;
                    }

                    if (currentRound && currentRound.id !== undefined && currentRound.id !== null) {
                        roundClientAnchors.delete(String(currentRound.id));
                    }
                    pendingFreshRoundVisual = true;
                    currentRound = null;
                    hideAll();
                    resetBet();
                    showCrashStatus('Загрузка раунда...', false);

                    setTimeout(async () => {
                        if (!crashActive) return;
                        await syncCrashRound(true).catch(() => {});
                        if (!currentRound && crashActive) {
                            setTimeout(() => {
                                if (crashActive) syncCrashRound(true).catch(() => {});
                            }, 900);
                        }
                    }, 120);
                }
            }, 50);
        }

        // ─── Cashout ───
        function updateCash(m) {
            const btn = document.getElementById('crashBetBtn');
            const w = Math.floor(currentBet * m);
            if (!cashoutInit) {
                btn.innerHTML = 'Забрать <span id="cashAmt">'+w+'</span> <img src="star.png" alt="">';
                cashoutInit = true;
                btn.classList.remove('placed','disabled');
                btn.onclick = function(){ doCash(); };
            } else { const e = document.getElementById('cashAmt'); if (e) e.textContent = w; }
        }

        function updateGrow(m) {
            const e = document.querySelector('.crash-player-row.me .cp-growing-amount');
            if (e) e.textContent = Math.floor(currentBet * m);
        }

        function doCash() {
            if (!crashRunning || !currentRound || crashShown || didCashOut) return;
            const now = srvNow();
            const { gameStart: gs } = getRoundTimes(currentRound);
            const m = mult(now - gs);
            const w = Math.floor(currentBet * m);
            const myKey = crashBetKey({ round_id: currentRound.id, user_id: myUserId });
            const existing = crashBetsState.get(myKey);
            if (existing) {
                existing.status = 'won';
                existing.cashout_multiplier = m;
                existing.cashout_amount = w;
                crashBetsState.set(myKey, existing);
                renderBetRecord(existing);
            }
            setBtnState('Забрали '+w+' <img src="star.png" alt="">','placed');
            betPlaced = false; didCashOut = true; cashoutInit = false;
            if (sb) sb.from('crash_bets').update({ cashout_multiplier: m, cashout_amount: w, status:'won' }).eq('round_id', currentRound.id).eq('user_id', myUserId).then(()=>{});
            if (wonGift) showGiftModal(wonGift, w); else setBalance(getBalance() + w, { immediate: true });
        }

        // ─── Submit Bet ───
        async function submitBetCrash() {
            const amt = parseInt(document.getElementById('betAmount').value) || 0;
            if (amt <= 0 || amt > getBalance() || !currentRound || !sb) return;
            clearStickyGiftPreview();
            const now = srvNow();
            const { gameStart: gs } = getRoundTimes(currentRound);
            if (now >= gs) return; // too late

            const prevBalance = getBalance();
            const myKey = crashBetKey({ round_id: currentRound.id, user_id: myUserId });

            setBalance(prevBalance - amt, { immediate: true });
            currentBet = amt;
            betPlaced = true;
            didCashOut = false;
            cashoutInit = false;
            closeBetMenu();
            setBtnState('Ставка отправляется...','disabled');
            showMyBet(amt);

            const { error } = await sb.from('crash_bets').upsert(
                {
                    round_id: currentRound.id,
                    user_id: myUserId,
                    user_name: myName && myName !== 'User' ? myName : (myName || ''),
                    user_photo: myPhoto || '',
                    amount: amt,
                    status: 'active'
                },
                { onConflict: 'round_id,user_id' }
            );

            if (error) {
                console.error('submitBetCrash failed', error);
                setBalance(prevBalance, { immediate: true });
                currentBet = 0;
                betPlaced = false;
                didCashOut = false;
                cashoutInit = false;
                const row = document.querySelector('[data-bet-key="' + escapeHtml(myKey) + '"]');
                if (row) row.remove();
                const btn = document.getElementById('crashBetBtn');
                if (btn) {
                    btn.innerHTML = 'Сделать ставку';
                    btn.classList.remove('placed', 'disabled');
                    btn.onclick = function(){ placeBet(); };
                }
                return;
            }

            setBtnState('Ставка сделана','placed');
        }

        // ─── Crash offline state ───
        function localRound() {
            showCrashStatus('Локальный режим отключён', true);
        }

        // ─── Gift System ───
        const giftList = [
            {name:'Amber Adder',img:'Gifts/Amber Adder.png',price:475},
            {name:'Citrone',img:'Gifts/Citrone.png',price:519},
            {name:'Amethyst',img:'Gifts/Amethyst.png',price:525},
            {name:'Anniversary',img:'Gifts/Anniversary.png',price:545},
            {name:'Albino',img:'Gifts/Albino.png',price:550},
            {name:'Chuckle Crown',img:'Gifts/Chuckle Crown.png',price:559},
            {name:'Absinthe',img:'Gifts/Absinthe.png',price:575},
            {name:'Azurite',img:'Gifts/Azurite.png',price:575},
            {name:'Be Awesome!',img:'Gifts/Be Awesome!.png',price:575},
            {name:'Baked Logo',img:'Gifts/Baked Logo.png',price:578},
            {name:'Backyard',img:'Gifts/Backyard.png',price:579},
            {name:'Bronze Age',img:'Gifts/Bronze Age.png',price:583},
            {name:'Carrot Cake',img:'Gifts/Carrot Cake.png',price:583},
            {name:'Cheesecake',img:'Gifts/Cheesecake.png',price:583},
            {name:'Choco Chips',img:'Gifts/Choco Chips.png',price:583},
            {name:'Citrus',img:'Gifts/Citrus.png',price:583},
            {name:'Citrus Fresh',img:'Gifts/Citrus Fresh.png',price:584},
            {name:'Court Jester',img:'Gifts/Court Jester.png',price:584},
            {name:'Dark Cherry',img:'Gifts/Dark Cherry.png',price:584},
            {name:'Bronze',img:'Gifts/Bronze.png',price:585},
            {name:'Cyber Ruby',img:'Gifts/Cyber Ruby.png',price:585},
            {name:'Black Ink',img:'Gifts/Black Ink.png',price:599},
            {name:'Canceled',img:'Gifts/Canceled.png',price:647},
            {name:'Adam',img:'Gifts/Adam.png',price:650},
            {name:'Blue Beam',img:'Gifts/Blue Beam.png',price:650},
            {name:'Candyman',img:'Gifts/Candyman.png',price:667},
            {name:'Apple Fresh',img:'Gifts/Apple Fresh.png',price:684},
            {name:'Alpine Fern',img:'Gifts/Alpine Fern.png',price:707},
            {name:'Cycloppy',img:'Gifts/Cycloppy.png',price:713},
            {name:'Angry Ghost',img:'Gifts/Angry Ghost.png',price:743},
            {name:'Chamomile',img:'Gifts/Chamomile.png',price:784},
            {name:'Affection',img:'Gifts/Affection.png',price:814},
            {name:'Arlequin',img:'Gifts/Arlequin.png',price:821},
            {name:'Anno Domini',img:'Gifts/Anno Domini.png',price:825},
            {name:'Blue Steel',img:'Gifts/Blue Steel.png',price:825},
            {name:'April Fools',img:'Gifts/April Fools.png',price:833},
            {name:'Dark Secret',img:'Gifts/Dark Secret.png',price:833},
            {name:'Berry Button',img:'Gifts/Berry Button.png',price:835},
            {name:'8 Ball',img:'Gifts/8 Ball.png',price:867},
            {name:'Brand New',img:'Gifts/Brand New.png',price:875},
            {name:'Berryllium',img:'Gifts/Berryllium.png',price:949},
            {name:'Antique',img:'Gifts/Antique.png',price:978},
            {name:'Barbie',img:'Gifts/Barbie.png',price:987},
            {name:'Carambola',img:'Gifts/Carambola.png',price:1030},
            {name:'Cone of Cold',img:'Gifts/Cone of Cold.png',price:1054},
            {name:'Azalea',img:'Gifts/Azalea.png',price:1066},
            {name:'Amphibian',img:'Gifts/Amphibian.png',price:1069},
            {name:'Alpha',img:'Gifts/Alpha.png',price:1147},
            {name:'Anatomy',img:'Gifts/Anatomy.png',price:1173},
            {name:'Bronze',img:'Gifts/Bronze.png',price:1200},
            {name:'Butterfly Tie',img:'Gifts/Butterfly Tie.png',price:1248},
            {name:'Balloon Face',img:'Gifts/Balloon Face.png',price:1250},
            {name:'Berry Blaster',img:'Gifts/Berry Blaster.png',price:1400},
            {name:'Amanita',img:'Gifts/Amanita.png',price:1403},
            {name:'Butterflight',img:'Gifts/Butterflight.png',price:1416},
            {name:'Acid Trip',img:'Gifts/Acid Trip.png',price:1441},
            {name:'Crash Test',img:'Gifts/Crash Test.png',price:1498},
            {name:'Gouda Wax',img:'Gifts/Gouda Wax.png',price:1579},
            {name:'Bee Movie',img:'Gifts/Bee Movie.png',price:1632},
            {name:'Astro Bot',img:'Gifts/Astro Bot.png',price:1667},
            {name:'Billiard',img:'Gifts/Billiard.png',price:1755},
            {name:'Afterglow',img:'Gifts/Afterglow.png',price:1807},
            {name:'Alien Script',img:'Gifts/Alien Script.png',price:1888},
            {name:'Alchemy',img:'Gifts/Alchemy.png',price:2040},
            {name:'Bubble Bath',img:'Gifts/Bubble Bath.png',price:2333},
            {name:'Amortentia',img:'Gifts/Amortentia.png',price:2452},
            {name:'Abyss Heart',img:'Gifts/Abyss Heart.png',price:2454},
            {name:'Black Hole',img:'Gifts/Black Hole.png',price:2779},
            {name:'Airy Souffle',img:'Gifts/Airy Souffle.png',price:3186},
            {name:'Aqua Gem',img:'Gifts/Aqua Gem.png',price:3244},
            {name:'Aquaviolet',img:'Gifts/Aquaviolet.png',price:3285},
            {name:'Disco',img:'Gifts/Disco.png',price:3329},
            {name:'Candy Cane',img:'Gifts/Candy Cane.png',price:3334},
            {name:'3D Glow',img:'Gifts/3D Glow.png',price:3715},
            {name:'Creepy Po',img:'Gifts/Creepy Po.png',price:4273},
            {name:'Banana Pox',img:'Gifts/Banana Pox.png',price:5255},
            {name:'Cruella',img:'Gifts/Cruella.png',price:5567},
            {name:'Blue Neon',img:'Gifts/Blue Neon.png',price:5582},
            {name:'Cheshire Cat',img:'Gifts/Cheshire Cat.png',price:6264},
            {name:'Baller',img:'Gifts/Baller.png',price:6273},
            {name:'Frogtart',img:'Gifts/Frogtart.png',price:7097},
            {name:'Cartoon Sky',img:'Gifts/Cartoon Sky.png',price:9277},
            {name:'Cartoon',img:'Gifts/Cartoon.png',price:10016},
            {name:'Mad Magenta',img:'Gifts/Mad Magenta.png',price:10024},
            {name:'Amazon',img:'Gifts/Amazon.png',price:10265},
            {name:'Adult Tasks',img:'Gifts/Adult Tasks.png',price:10731},
            {name:'Aurora Joy',img:'Gifts/Aurora Joy.png',price:11419},
            {name:'Angry Amethyst',img:'Gifts/Angry Amethyst.png',price:11498},
            {name:'Brainiac',img:'Gifts/Brainiac.png',price:12531},
            {name:'Agent Orange',img:'Gifts/Agent Orange.png',price:16343},
            {name:'Boingo',img:'Gifts/Boingo.png',price:18089},
            {name:'Creeper',img:'Gifts/Creeper.png',price:18806},
            {name:'Bogartite',img:'Gifts/Bogartite.png',price:24445},
            {name:'Academic',img:'Gifts/Academic.png',price:24666},
            {name:'Barbed',img:'Gifts/Barbed.png',price:27471},
            {name:'Cozy Warmth',img:'Gifts/Cozy Warmth.png',price:39284},
            {name:'Brewtoad',img:'Gifts/Brewtoad.png',price:41735},
            {name:'Ardente',img:'Gifts/Ardente.png',price:48395},
            {name:'Liberty',img:'Gifts/Liberty.png',price:50111},
            {name:'Bluebird',img:'Gifts/Bluebird.png',price:92573},
            {name:'Action Film',img:'Gifts/Action Film.png',price:145452},
        ];
        const giftCatalogMap = new Map(giftList.map(g => [String(g.name) + '||' + String(g.img), true]));
        let currentGiftIdx = -1, wonGift = null, inventory = [];

        function sanitizeInventoryItem(raw) {
            if (!raw) return null;
            const name = String(raw.name ?? raw.gift_name ?? '').trim();
            const img = String(raw.img ?? raw.gift_img ?? '').trim();
            const price = Math.max(0, parseInt(raw.price ?? raw.gift_price, 10) || 0);
            const id = raw.id ?? null;
            if (!name || !img || !price) return null;
            if (!giftCatalogMap.has(name + '||' + img)) return null;
            return { id, name, img, price };
        }

        function updateGift(w) {
            const row = document.querySelector('.crash-player-row.me');
            trackMyPotentialGift(w);
            if (row) setPlayerGift(row, w, 'active');
        }


        function removeStickyGiftPreviewRow() {
            const row = document.querySelector('.crash-player-row.sticky-preview');
            if (row) row.remove();
        }

        function removeMyWonPreviewRows() {
            document.querySelectorAll('.crash-player-row.me:not(.sticky-preview)').forEach(row => {
                if (row.classList.contains('status-won')) row.remove();
            });
        }

        function renderStickyGiftPreview() {
            const players = document.getElementById('crashPlayers');
            const wait = document.getElementById('crashWaitingText');
            if (!players) return;

            removeStickyGiftPreviewRow();
            removeMyWonPreviewRows();

            if (!stickyMyGiftPreview || betPlaced) {
                if (wait && !players.querySelector('.crash-player-row')) {
                    wait.style.display = '';
                    wait.textContent = 'Ожидание ставок';
                }
                return;
            }

            const avatar = myPhoto
                ? '<img class="cp-avatar" src="' + escapeHtml(myPhoto) + '" alt="">'
                : '<div class="cp-avatar-placeholder">' + escapeHtml((myName || 'В')[0]) + '</div>';

            const amount = Math.max(0, Math.floor(Number(stickyMyGiftPreview.amount) || 0));
            const markup = '<div class="crash-player-row me status-won sticky-preview" data-bet-key="sticky-me-gift">' +
                avatar +
                '<div class="cp-name"><span class="cp-name-text">Вы</span><span class="cp-original-bet">Подарок</span></div>' +
                '<span class="cp-amount"><span class="cp-growing-amount">' + amount + '</span> <img src="star.png" alt=""></span>' +
                '<img class="cp-gift" src="" alt="" style="display:none;">' +
                '</div>';

            const wrap = document.createElement('div');
            wrap.innerHTML = markup;
            const row = wrap.firstChild;
            players.prepend(row);
            setPlayerGift(row, amount, 'won');

            if (wait) {
                wait.style.display = 'none';
            }
        }

        function rememberStickyGiftPreview(item) {
            if (!item) return;
            stickyMyGiftPreview = {
                amount: Math.max(0, Math.floor(Number(item.price) || 0)),
                gift: { name: item.name || '', img: item.img || '' }
            };
            renderStickyGiftPreview();
        }

        function clearStickyGiftPreview() {
            stickyMyGiftPreview = null;
            removeStickyGiftPreviewRow();
        }

        function resetGiftDisplay() {
            currentGiftIdx = -1;
            wonGift = null;
            const row = document.querySelector('.crash-player-row.me:not(.sticky-preview)');
            if (row) setPlayerGift(row, 0, 'lost');
        }

        function showGiftModal(gift, w) {
            const m = document.getElementById('giftModal');
            if (!m) return;
            // Не перекрываем модал пока предыдущее действие в процессе
            if (m.dataset.busy === '1') return;
            const img = document.getElementById('giftModalImg');
            img.classList.add('loading');
            img.onload = function() { img.classList.remove('loading'); };
            img.onerror = function() { img.classList.add('loading'); };
            img.src = encodeURI(gift.img);
            document.getElementById('giftModalName').textContent = gift.name;
            document.getElementById('giftSellAmount').textContent = w;
            m.classList.add('show'); m._gift = gift; m._win = w; m._roundId = currentRound?.id || null; m.dataset.busy = '0';
        }
        async function sellGift() {
            const m = document.getElementById('giftModal');
            if (!m || m.dataset.busy === '1') return;
            const safeWin = Math.max(0, parseInt(m._win, 10) || 0);
            if (!safeWin) return;
            m.dataset.busy = '1';
            clearStickyGiftPreview();
            try {
                const result = await apiRequest('/api/crash/reward/sell', { body: { round_id: m._roundId, amount: safeWin } });
                if (result?.ok) {
                    setBalance(result.balance || 0, { persist: false, cache: true });
                    m.classList.remove('show');
                    m._gift = null; m._win = 0; m._roundId = null;
                } else {
                    alert('Не удалось продать награду');
                }
            } catch (e) {
                console.error('sellGift error', e);
                alert('Ошибка при продаже награды');
            } finally {
                setTimeout(() => { m.dataset.busy = '0'; }, 250);
            }
        }
        let _giftBeingTaken = false;
        async function takeGift() {
            const m = document.getElementById('giftModal');
            if (!m || !m._gift || m.dataset.busy === '1' || _giftBeingTaken) return;

            const expectedGift = sanitizeInventoryItem({
                id: null,
                name: m._gift?.name || '',
                img: m._gift?.img || '',
                price: Math.max(0, parseInt(m._win, 10) || 0)
            });
            const expectedPair = expectedGift ? (String(expectedGift.name) + '||' + String(expectedGift.img)) : '';
            const myKey = crashBetKey({ round_id: m._roundId, user_id: myUserId });

            _giftBeingTaken = true;
            m.dataset.busy = '1';
            m.classList.remove('show');

            try {
                const result = await apiRequest('/api/crash/reward/take', { body: { round_id: m._roundId } });
                let savedItem = sanitizeInventoryItem(result?.item);

                if (!savedItem) {
                    await refreshInventoryFromDB();
                    savedItem = inventory.find(it => (String(it.name) + '||' + String(it.img)) === expectedPair) || null;
                }

                if (!savedItem && expectedGift) {
                    throw new Error('TAKE_GIFT_NOT_CONFIRMED');
                }

                const existingBet = crashBetsState.get(myKey);
                if (existingBet) {
                    existingBet.status = 'won';
                    existingBet.reward_claimed = true;
                    existingBet.reward_type = 'gift';
                    existingBet.cashout_amount = existingBet.cashout_amount || Math.max(0, parseInt(m._win, 10) || 0);
                    crashBetsState.set(myKey, existingBet);
                }

                m._gift = null; m._win = 0; m._roundId = null;

                if (savedItem) {
                    const alreadyExists = inventory.some(inv => inv && inv.id && String(inv.id) === String(savedItem.id));
                    if (!alreadyExists) {
                        inventory.unshift(savedItem);
                    }
                    renderInventory();
                }
            } catch (e) {
                console.error('takeGift error', e);
                await refreshInventoryFromDB();
                const recoveredItem = expectedPair ? inventory.find(it => (String(it.name) + '||' + String(it.img)) === expectedPair) : null;
                if (!recoveredItem) {
                    alert('Ошибка при получении подарка');
                }
            } finally {
                m.dataset.busy = '0';
                _giftBeingTaken = false;
            }
        }

        function getInventoryCountLabel(n) {
            const count = Math.max(0, parseInt(n, 10) || 0);
            const mod10 = count % 10;
            const mod100 = count % 100;
            if (mod10 === 1 && mod100 !== 11) return count + ' подарок';
            if (mod10 >= 2 && mod10 <= 4 && (mod100 < 12 || mod100 > 14)) return count + ' подарка';
            return count + ' подарков';
        }

        function updateInventorySummary() {
            const countEl = document.getElementById('inventoryCountText');
            const btn = document.getElementById('sellAllBtn');
            const total = inventory.reduce((sum, it) => sum + (Math.max(0, parseInt(it && it.price, 10) || 0)), 0);
            if (countEl) countEl.textContent = getInventoryCountLabel(inventory.length);
            if (btn) {
                btn.innerHTML = getSellAllLabel(total) + ' <img src="star.png" alt="">';
                const disabled = inventory.length === 0 || total <= 0;
                btn.disabled = disabled;
                btn.classList.toggle('disabled', disabled);
            }
        }

        async function sellAllInventory() {
            const itemsToSell = inventory.map(sanitizeInventoryItem).filter(it => it && it.id);
            if (!itemsToSell.length) return;

            const prevBalance = getBalance();
            const prevInventory = inventory.slice();
            const total = itemsToSell.reduce((s, it) => s + it.price, 0);

            setBalance(prevBalance + total, { immediate: true });
            inventory = [];
            renderInventory();

            try {
                const results = await Promise.all(itemsToSell.map(it => dbRemoveInvItem(it.id)));
                if (results.some(ok => !ok)) throw new Error('SELL_ALL_FAILED');
                await refreshBalanceFromDB();
            } catch (e) {
                console.error('sellAllInventory failed', e);
                inventory = prevInventory;
                setBalance(prevBalance, { immediate: true });
                renderInventory();
                await refreshInventoryFromDB();
                await refreshBalanceFromDB();
                alert('Не удалось продать часть подарков');
            }
        }

        function renderInventory() {
            const grid = document.getElementById('inventoryGrid');
            const empty = document.getElementById('emptyInventory');
            inventory = inventory.map(sanitizeInventoryItem).filter(Boolean);
            reconcileCraftSelection();
            updateInventorySummary();

            if (!inventory.length) {
                empty.style.display = 'flex';
                grid.innerHTML = '';
                return;
            }

            empty.style.display = 'none';
            grid.innerHTML = '';

            inventory.forEach((it, i) => {
                const itemEl = document.createElement('div');
                itemEl.className = 'inventory-item';

                const imgEl = document.createElement('img');
                imgEl.className = 'inv-img loading';
                imgEl.alt = '';
                imgEl.src = encodeURI(it.img);
                imgEl.onload = function() { imgEl.classList.remove('loading'); };
                imgEl.onerror = function() { imgEl.classList.add('loading'); };

                const nameEl = document.createElement('div');
                nameEl.className = 'inv-name';
                nameEl.textContent = it.name;

                const btnsEl = document.createElement('div');
                btnsEl.className = 'inv-btns';

                const sellBtn = document.createElement('button');
                sellBtn.className = 'inv-btn sell';
                sellBtn.type = 'button';
                sellBtn.innerHTML = 'Продать ' + it.price + ' <img src="star.png" alt="">';
                sellBtn.onclick = function() { sellInvItem(i); };

                const withdrawBtn = document.createElement('button');
                withdrawBtn.className = 'inv-btn withdraw';
                withdrawBtn.type = 'button';
                withdrawBtn.textContent = 'Вывести';

                btnsEl.appendChild(sellBtn);
                btnsEl.appendChild(withdrawBtn);
                itemEl.appendChild(imgEl);
                itemEl.appendChild(nameEl);
                itemEl.appendChild(btnsEl);
                grid.appendChild(itemEl);
            });
        }
        async function sellInvItem(i) {
            const it = sanitizeInventoryItem(inventory[i]);
            if (!it || !it.id) { await refreshInventoryFromDB(); return; }

            const prevBalance = getBalance();
            const prevInventory = inventory.slice();

            setBalance(prevBalance + it.price, { immediate: true });
            inventory.splice(i, 1);
            renderInventory();

            try {
                const ok = await dbRemoveInvItem(it.id);
                if (!ok) throw new Error('SELL_ITEM_FAILED');
                await refreshBalanceFromDB();
            } catch (e) {
                console.error('sellInvItem failed', e);
                inventory = prevInventory;
                setBalance(prevBalance, { immediate: true });
                renderInventory();
                await refreshInventoryFromDB();
                await refreshBalanceFromDB();
                alert('Не удалось продать подарок');
            }
        }

        // ─── Bet Menu ───
        function placeBet() {
            if (!currentRound) return;
            const now = srvNow();
            const times = getRoundTimes(currentRound);
            if (crashRunning || crashShown || now >= times.gameStart) return;
            document.getElementById('betMenu').classList.add('show');
            document.getElementById('betOverlay').classList.add('show');
        }
        function closeBetMenu() { document.getElementById('betMenu').classList.remove('show'); document.getElementById('betOverlay').classList.remove('show'); }

        // ─── Banner ───
        function openBanner(url) { if (tg && tg.openTelegramLink) tg.openTelegramLink(url); else window.open(url, '_blank'); }

        // ─── Referral ───
        function showReferralPage() { document.getElementById('page-profile').classList.remove('active'); document.getElementById('page-referral').classList.add('active'); refreshReferralStats(); }
        function hideReferralPage() { document.getElementById('page-referral').classList.remove('active'); document.getElementById('page-profile').classList.add('active'); }
        function copyRefLink() {
            const link = document.getElementById('refLinkInput').value;
            if (navigator.clipboard) navigator.clipboard.writeText(link); else { document.getElementById('refLinkInput').select(); document.execCommand('copy'); }
            const b = document.querySelector('.ref-copy-btn'); if (b) { b.dataset.copied = '1'; b.textContent = t('copied'); setTimeout(() => { b.dataset.copied = '0'; b.textContent = t('copy_link'); }, 1500); }
        }

        // ─── Settings ───
        function openSettings() { document.getElementById('settingsModal').classList.add('show'); }
        function closeSettings() { document.getElementById('settingsModal').classList.remove('show'); document.getElementById('langOptions').classList.remove('show'); document.getElementById('langRow').classList.remove('open'); }
        const LANG_STORAGE_KEY = 'giftpepe_lang';
        let currentLang = localStorage.getItem(LANG_STORAGE_KEY) || 'ru';
        const I18N = {
            ru: { top:'Топ', games:'Игры', profile_nav:'Профиль', choose_game:'Выберите игру', profile:'Профиль', inventory:'Инвентарь', promo_title:'Промокод', promo_placeholder:'Введите промокод', apply:'Применить', invite_friend:'ПРИГЛАСИТЬ ДРУГА', invite_sub:'Получайте бонусы', back:'Назад', referrals:'Рефералы', earned:'Заработано', copy_link:'Скопировать ссылку', copied:'Скопировано!', promo_enter:'Введите промокод', promo_unavailable:'Промокоды пока недоступны', settings_language:'Язык', settings_support:'Поддержка', settings_save:'Сохранить и продолжить', referral_title:'РЕФЕРАЛЬНАЯ СИСТЕМА', referral_desc:'За каждого приглашённого друга вы получаете 10% от его пополнений', gifts_empty_title:'Не нашли подарков?', gifts_empty_desc:'Пополните в @GiftPepeSupport или выиграйте в играх.', gift_sell:'Продать', gift_take:'Забрать', sell_all:'Продать все', withdraw:'Вывести', topup_title:'Пополнение', bet_title:'Ставка', place_bet:'Сделать ставку', waiting_bets:'Ожидание ставок', check_label:'Чек', check_subtitle:'Нажми на кнопку ниже, чтобы забрать чек', check_claim:'Получить', check_claimed:'Получено', check_claiming:'Получаем...', check_loading:'Загружаем чек...', check_unavailable:'Недоступно', check_load_failed:'Не удалось загрузить', toast_success:'Успешно', toast_error:'Ошибка', toast_received:'Получено', toast_claim_failed:'Не удалось получить', gift_count_1:'подарок', gift_count_2:'подарка', gift_count_5:'подарков', craft_add_more:'Добавьте еще', craft_gifts_word:'подарка', craft_value_label:'Стоимость ваших подарков', craft_add_own:'Добавить свои подарки', topup_history_title:'История оплат', topup_history_empty:'История оплат пуста' },
            en: { top:'Top', games:'Games', profile_nav:'Profile', choose_game:'Choose a game', profile:'Profile', inventory:'Inventory', promo_title:'Promo code', promo_placeholder:'Enter promo code', apply:'Apply', invite_friend:'INVITE A FRIEND', invite_sub:'Get bonuses', back:'Back', referrals:'Referrals', earned:'Earned', copy_link:'Copy link', copied:'Copied!', promo_enter:'Enter promo code', promo_unavailable:'Promo codes are not available yet', settings_language:'Language', settings_support:'Support', settings_save:'Save and continue', referral_title:'REFERRAL SYSTEM', referral_desc:'For each invited friend, you receive 10% of their top-ups', gifts_empty_title:'No gifts found?', gifts_empty_desc:'Top up in @GiftPepeSupport or win in games.', gift_sell:'Sell', gift_take:'Collect', sell_all:'Sell all', withdraw:'Withdraw', topup_title:'Top up', bet_title:'Bet', place_bet:'Place bet', waiting_bets:'Waiting for bets', check_label:'Check', check_subtitle:'Tap the button below to claim the check', check_claim:'Claim', check_claimed:'Claimed', check_claiming:'Claiming...', check_loading:'Loading check...', check_unavailable:'Unavailable', check_load_failed:'Failed to load', toast_success:'Success', toast_error:'Error', toast_received:'Received', toast_claim_failed:'Failed to claim', gift_count_1:'gift', gift_count_2:'gifts', gift_count_5:'gifts', craft_add_more:'Add', craft_gifts_word:'more gifts', craft_value_label:'Value of your gifts', craft_add_own:'Add your gifts', topup_history_title:'Payment history', topup_history_empty:'Payment history is empty' }
        };
        function t(key) { const dict = I18N[currentLang] || I18N.ru; return dict[key] || I18N.ru[key] || key; }
        function getInventoryCountLabel(count) { const n = Math.max(0, parseInt(count, 10) || 0); if (currentLang === 'en') return n + ' ' + (n === 1 ? t('gift_count_1') : t('gift_count_2')); const mod10 = n % 10; const mod100 = n % 100; let word = t('gift_count_5'); if (mod10 === 1 && mod100 !== 11) word = t('gift_count_1'); else if (mod10 >= 2 && mod10 <= 4 && !(mod100 >= 12 && mod100 <= 14)) word = t('gift_count_2'); return n + ' ' + word; }
        function getSellAllLabel(total) { return t('sell_all') + ' ' + (Math.max(0, parseInt(total, 10) || 0)); }
        function updateGiftModalTranslations() { const sellAmount = document.getElementById('giftSellAmount')?.textContent || ''; const sellBtn = document.getElementById('giftSellBtn'); const takeBtn = document.getElementById('giftTakeBtn'); if (sellBtn) sellBtn.innerHTML = t('gift_sell') + ' <span id="giftSellAmount">' + sellAmount + '</span> <img src="star.png" alt="">'; if (takeBtn) takeBtn.textContent = t('gift_take'); }
        function updateCheckTranslations() { const subtitle = document.getElementById('checkSubtitle'); const label = document.getElementById('checkLabel'); if (label) label.textContent = t('check_label'); if (subtitle && (!subtitle.textContent || subtitle.dataset.i18n !== 'manual')) subtitle.textContent = t('check_subtitle'); }
        function applyTranslations() { document.documentElement.lang = currentLang; const navSpans = document.querySelectorAll('.nav-item span'); if (navSpans[0]) navSpans[0].textContent = t('top'); if (navSpans[1]) navSpans[1].textContent = t('games'); if (navSpans[2]) navSpans[2].textContent = t('profile_nav'); const gameTitle = document.querySelector('#page-games .section-title'); if (gameTitle) gameTitle.textContent = t('choose_game'); const profileTitle = document.querySelector('#page-profile > .section-title'); if (profileTitle) profileTitle.textContent = t('profile'); const invTitle = document.querySelector('.inventory-header .section-title'); if (invTitle) invTitle.textContent = t('inventory'); const promoTitle = document.getElementById('promoTitle'); const promoInput = document.getElementById('promoInput'); const promoApplyBtn = document.getElementById('promoApplyBtn'); if (promoTitle) promoTitle.textContent = t('promo_title'); if (promoInput) promoInput.placeholder = t('promo_placeholder'); if (promoApplyBtn) promoApplyBtn.textContent = t('apply'); const inviteMain = document.querySelector('.invite-btn-text .main'); const inviteSub = document.querySelector('.invite-btn-text .sub'); if (inviteMain) inviteMain.textContent = t('invite_friend'); if (inviteSub) inviteSub.textContent = t('invite_sub'); const refBack = document.querySelector('.ref-back span'); if (refBack) refBack.textContent = t('back'); const crashBack = document.getElementById('crashBackLabel'); if (crashBack) crashBack.textContent = t('back'); const craftBack = document.getElementById('craftBackLabel'); if (craftBack) craftBack.textContent = t('back'); const refLabels = document.querySelectorAll('.ref-stat-card .label'); if (refLabels[0]) refLabels[0].textContent = t('referrals'); if (refLabels[1]) refLabels[1].textContent = t('earned'); const copyBtn = document.querySelector('.ref-copy-btn'); if (copyBtn && copyBtn.dataset.copied !== '1') copyBtn.textContent = t('copy_link'); const langRowLabel = document.getElementById('langRowLabel'); const supportRowLabel = document.getElementById('supportRowLabel'); const settingsSaveBtn = document.getElementById('settingsSaveBtn'); if (langRowLabel) langRowLabel.textContent = t('settings_language'); if (supportRowLabel) supportRowLabel.textContent = t('settings_support'); if (settingsSaveBtn) settingsSaveBtn.textContent = t('settings_save'); const refCardTitle = document.getElementById('refCardTitle'); const refCardDesc = document.getElementById('refCardDesc'); if (refCardTitle) refCardTitle.textContent = t('referral_title'); if (refCardDesc) refCardDesc.textContent = t('referral_desc'); const giftsEmptyTitle = document.getElementById('giftsEmptyTitle'); const giftsEmptyDesc = document.getElementById('giftsEmptyDesc'); if (giftsEmptyTitle) giftsEmptyTitle.textContent = t('gifts_empty_title'); if (giftsEmptyDesc) giftsEmptyDesc.innerHTML = t('gifts_empty_desc').replace('@GiftPepeSupport', '<a href="https://t.me/GiftPepeSupport" target="_blank">@GiftPepeSupport</a>'); const crashBetBtn = document.getElementById('crashBetBtn'); const crashWaitingText = document.getElementById('crashWaitingText'); const topupTitleEl = document.getElementById('topupTitleEl'); const betMenuTitle = document.getElementById('betMenuTitle'); const betSubmitBtn = document.getElementById('betSubmitBtn'); if (crashBetBtn) crashBetBtn.textContent = t('place_bet'); if (crashWaitingText) crashWaitingText.textContent = t('waiting_bets'); if (topupTitleEl) topupTitleEl.textContent = topupHistoryOpen ? t('topup_history_title') : t('topup_title'); if (betMenuTitle) betMenuTitle.textContent = t('bet_title'); if (betSubmitBtn) betSubmitBtn.textContent = t('place_bet'); const craftLabel = document.querySelector('.craft-summary-label'); if (craftLabel) craftLabel.textContent = t('craft_value_label'); const craftAddBtn = document.getElementById('craftAddBtn'); if (craftAddBtn) craftAddBtn.textContent = t('craft_add_own'); if (typeof renderCraftUI === 'function') renderCraftUI(); updateCheckTranslations(); updateGiftModalTranslations(); if (typeof updateInventorySummary === 'function') updateInventorySummary(); if (typeof renderInventory === 'function') renderInventory(); if (typeof renderTopLeaderboard === 'function') renderTopLeaderboard(topLeaderboardCache); if (typeof renderTopupHistory === 'function') renderTopupHistory(topupHistoryCache); if (typeof syncTopupTitle === 'function') syncTopupTitle(); }
        function closeSettingsOutside(e) { if (e.target === e.currentTarget) closeSettings(); }
        function toggleLang() { document.getElementById('langOptions').classList.toggle('show'); document.getElementById('langRow').classList.toggle('open'); }
        function selectLang(lang, el) { currentLang = I18N[lang] ? lang : 'ru'; localStorage.setItem(LANG_STORAGE_KEY, currentLang); document.querySelectorAll('.lang-pill').forEach(p => p.classList.remove('active')); if (el) el.classList.add('active'); applyTranslations(); }
        function openSupport() { closeSettings(); if (tg && tg.openTelegramLink) tg.openTelegramLink('https://t.me/GiftPepeSupport'); else window.open('https://t.me/GiftPepeSupport','_blank'); }
        // ─── Promo Code (API) ───
        async function applyPromoCode() {
            const input = document.getElementById('promoInput');
            const code  = (input?.value || '').trim();
            if (!code) { alert(t('promo_enter')); return; }
            if (!myUserId) { alert('Авторизуйся через Telegram'); return; }

            const applyBtn = document.getElementById('promoApplyBtn');
            if (applyBtn) { applyBtn.disabled = true; applyBtn.textContent = '...'; }

            try {
                await ensureBackendAuth();
                const result = await apiRequest('/api/promo/apply', { body: { code } });

                if (result?.error === 'not_found') {
                    alert('Промокод не найден');
                } else if (result?.error === 'used_up') {
                    alert('Промокод уже израсходован');
                } else if (result?.error === 'already_used') {
                    alert('Вы уже применяли этот промокод');
                } else if (result?.ok) {
                    setBalance(result.balance || 0, { immediate: false, persist: false, cache: true });
                    if (input) input.value = '';
                    showClaimToast(true, result.stars, t('toast_received'));
                    alert('✅ Промокод активирован! +' + result.stars + ' ⭐');
                    refreshReferralStats();
                    refreshTopLeaderboard(false);
                } else {
                    alert('Ошибка при применении промокода');
                }
            } catch (e) {
                console.error('applyPromoCode error', e);
                alert('Ошибка при применении промокода');
            } finally {
                if (applyBtn) { applyBtn.disabled = false; applyBtn.textContent = t('apply'); }
            }
        }

        (function initLanguage() {
            if (!I18N[currentLang]) currentLang = 'ru';
            document.querySelectorAll('.lang-pill').forEach(p => p.classList.toggle('active', p.dataset.lang === currentLang));
            applyTranslations();
        })();

        // ─── Top-up via Telegram Stars ───
        let _buyStarsLock = false;
        let topupHistoryOpen = false;
        let topupHistoryCache = [];
        let topupHistoryLoading = false;

        function getTopupStatusLabel(status) {
            const code = String(status || 'pending').toLowerCase();
            if (code === 'paid') return currentLang === 'en' ? 'Paid' : 'Оплачено';
            if (code === 'invoice_created') return currentLang === 'en' ? 'Invoice' : 'Счёт создан';
            if (code === 'failed') return currentLang === 'en' ? 'Failed' : 'Ошибка';
            if (code === 'cancelled') return currentLang === 'en' ? 'Cancelled' : 'Отменено';
            return currentLang === 'en' ? 'Pending' : 'В обработке';
        }

        function formatTopupDate(value) {
            if (!value) return '—';
            const date = new Date(value);
            if (Number.isNaN(date.getTime())) return '—';
            try {
                return date.toLocaleString(currentLang === 'en' ? 'en-GB' : 'ru-RU', {
                    day: '2-digit',
                    month: '2-digit',
                    year: 'numeric',
                    hour: '2-digit',
                    minute: '2-digit'
                });
            } catch (e) {
                return date.toISOString().slice(0, 16).replace('T', ' ');
            }
        }

        function syncTopupTitle() {
            const title = document.getElementById('topupTitleEl');
            const box = document.querySelector('#topupMenu .topup-box');
            if (!title || !box) return;
            title.textContent = topupHistoryOpen
                ? (currentLang === 'en' ? 'Payment history' : 'История оплат')
                : t('topup_title');
            box.classList.toggle('show-history', topupHistoryOpen);
            box.classList.toggle('show-main', !topupHistoryOpen);
        }

        function renderTopupHistory(items) {
            const list = document.getElementById('topupHistoryList');
            if (!list) return;

            const rows = Array.isArray(items) ? items.slice() : [];
            if (!rows.length) {
                list.innerHTML = '<div class="topup-history-empty" id="topupHistoryEmpty">' + (currentLang === 'en' ? 'Payment history is empty' : 'История оплат пуста') + '</div>';
                return;
            }

            list.innerHTML = rows.map(row => {
                const amount = Math.max(0, parseInt(row?.amount, 10) || 0);
                const status = String(row?.status || 'pending').toLowerCase();
                const dateText = formatTopupDate(row?.paid_at || row?.updated_at || row?.created_at);
                return '<div class="topup-history-row">' +
                    '<div class="topup-history-main">' +
                        '<div class="topup-history-amount">' + amount + ' <img src="star.png" alt=""></div>' +
                        '<div class="topup-history-date">' + escapeHtml(dateText) + '</div>' +
                    '</div>' +
                    '<div class="topup-history-status ' + escapeHtml(status) + '">' + escapeHtml(getTopupStatusLabel(status)) + '</div>' +
                '</div>';
            }).join('');
        }

        async function loadTopupHistory(force = false) {
            if (!sb || (topupHistoryLoading && !force)) return topupHistoryCache;
            topupHistoryLoading = true;
            try {
                const { data, error } = await sb.from('topups')
                    .select('id, amount, status, created_at, updated_at, paid_at')
                    .order('created_at', { ascending: false })
                    .limit(30);
                if (error) throw error;
                topupHistoryCache = Array.isArray(data) ? data : [];
                renderTopupHistory(topupHistoryCache);
                return topupHistoryCache;
            } catch (e) {
                console.error('loadTopupHistory failed', e);
                renderTopupHistory(topupHistoryCache);
                return topupHistoryCache;
            } finally {
                topupHistoryLoading = false;
            }
        }

        function openTopupHistory() {
            topupHistoryOpen = true;
            syncTopupTitle();
            loadTopupHistory(true).catch(() => {});
        }

        function closeTopupHistory() {
            topupHistoryOpen = false;
            syncTopupTitle();
        }

        function toggleTopupHistory() {
            if (topupHistoryOpen) closeTopupHistory();
            else openTopupHistory();
        }

        async function confirmTopupUntilSettled(topupKey, attempts = 8, delayMs = 1600) {
            let last = null;
            for (let i = 0; i < attempts; i++) {
                last = await apiRequest('/api/payments/confirm', { body: { topupKey } });
                if (last?.ok) return last;
                if (i < attempts - 1) await sleep(delayMs);
            }
            return last;
        }


        async function buyStars(amount) {
            const safeAmount = Math.max(1, parseInt(amount, 10) || 0);
            if (!safeAmount || _buyStarsLock) return;
            closeTopup();

            if (!tg) { alert('Открой приложение через Telegram'); return; }

            _buyStarsLock = true;

            const toast = null;

            try {
                const created = await apiRequest('/api/payments/create-invoice', { body: { amount: safeAmount } });
                try { toast.remove(); } catch (_) {}

                if (!created?.ok || !created.invoiceUrl) {
                    alert('Не удалось создать счёт');
                    _buyStarsLock = false;
                    return;
                }

                tg.openInvoice(created.invoiceUrl, async (status) => {
                    if (status === 'paid') {
                        try {
                            const settled = await confirmTopupUntilSettled(created.topupKey, 8, 1600);
                            if (settled?.ok) {
                                setBalance(settled.balance || 0, { persist: false, cache: true });
                                await loadTopupHistory(true);
                                refreshReferralStats();
                                refreshTopLeaderboard(true);
                                showClaimToast(true, safeAmount, '✅ Баланс пополнен');
                            } else {
                                await refreshBalanceFromDB();
                                await loadTopupHistory(true);
                                await refreshTopLeaderboard(true);
                                showClaimToast(true, safeAmount, '✅ Платёж подтверждён');
                            }
                        } catch (e) {
                            console.error('payment confirm failed', e);
                            await refreshBalanceFromDB();
                            await loadTopupHistory(true);
                            await refreshTopLeaderboard(true);
                            showClaimToast(true, safeAmount, '✅ Платёж подтверждён');
                        }
                    } else if (status === 'failed') {
                        showClaimToast(false, 0, 'Платёж не прошёл');
                    }
                    _buyStarsLock = false;
                });
            } catch (e) {
                try { toast.remove(); } catch(_) {}
                _buyStarsLock = false;
                console.error('buyStars error', e);
                alert('Ошибка: ' + (e.message || 'проверьте соединение'));
            }
        }

        function openTopup() {
            topupHistoryOpen = false;
            syncTopupTitle();
            document.getElementById('topupMenu').classList.add('show');
            document.getElementById('topupOverlay').classList.add('show');
        }
        function closeTopup() {
            topupHistoryOpen = false;
            syncTopupTitle();
            document.getElementById('topupMenu').classList.remove('show');
            document.getElementById('topupOverlay').classList.remove('show');
        }

        // ═══════════════════════════════════════════════════
        // ─── ADMIN PANEL ────────────────────────────────────
        // ═══════════════════════════════════════════════════

        function isAdmin() {
            return Array.isArray(ADMIN_IDS) && ADMIN_IDS.length > 0
                && myUserId && ADMIN_IDS.map(Number).includes(Number(myUserId));
        }

        function openAdmin() {
            if (!isAdmin()) {
                alert('⛔ Доступ запрещён');
                return;
            }
            document.getElementById('adminOverlay').classList.add('show');
            adminLoadPromos();
        }

        function closeAdmin() {
            document.getElementById('adminOverlay').classList.remove('show');
        }

        function adminOverlayClick(e) {
            if (e.target === document.getElementById('adminOverlay')) closeAdmin();
        }

        function adminMsg(elId, text, ok) {
            const el = document.getElementById(elId);
            if (!el) return;
            el.textContent = text;
            el.className = 'admin-msg ' + (ok ? 'ok' : 'err');
            clearTimeout(el._t);
            el._t = setTimeout(() => { el.textContent = ''; el.className = 'admin-msg'; }, 4000);
        }

        // ── Создать промокод ──
        async function adminCreatePromo() {
            const code  = (document.getElementById('ap-code')?.value || '').trim();
            const stars = parseInt(document.getElementById('ap-stars')?.value) || 0;
            const acts  = Math.max(1, parseInt(document.getElementById('ap-acts')?.value) || 1);

            if (!code)  { adminMsg('ap-msg', 'Введите код', false); return; }
            if (stars < 1) { adminMsg('ap-msg', 'Введите количество звёзд', false); return; }

            const btn = document.getElementById('ap-create-btn');
            btn.disabled = true; btn.textContent = 'Сохраняем...';

            try {
                await ensureBackendAuth();
                await apiRequest('/api/admin/promo/create', { body: { code, stars, activations_left: acts } });
                adminMsg('ap-msg', `✅ Промокод «${code}» создан (+${stars.toLocaleString()} ⭐, ${acts} акт.)`, true);
                document.getElementById('ap-code').value = '';
                document.getElementById('ap-stars').value = '';
                document.getElementById('ap-acts').value = '1';
                adminLoadPromos();
            } catch (e) {
                console.error('adminCreatePromo', e);
                adminMsg('ap-msg', 'Ошибка: ' + (e.message || 'неизвестная'), false);
            } finally {
                btn.disabled = false; btn.textContent = 'Создать промокод';
            }
        }

        // ── Загрузить список промокодов ──
        async function adminLoadPromos() {
            const listEl = document.getElementById('ap-list');
            if (!listEl) return;
            listEl.innerHTML = '<div class="admin-promo-empty">Загрузка...</div>';
            try {
                await ensureBackendAuth();
                const response = await apiRequest('/api/admin/promo/list');
                const rows = Array.isArray(response?.items) ? response.items : [];
                if (!rows.length) {
                    listEl.innerHTML = '<div class="admin-promo-empty">Промокодов нет</div>';
                    return;
                }
                listEl.innerHTML = '';
                rows.forEach(d => {
                    const left = parseInt(d.activations_left, 10) || 0;
                    const total = (Array.isArray(d.used_by) ? d.used_by.length : 0) + left;
                    const item = document.createElement('div');
                    item.className = 'admin-promo-item' + (left <= 0 ? ' exhausted' : '');
                    item.innerHTML =
                        `<div class="apc">
                            <div class="apc-code">${escapeHtml(d.code || '')}</div>
                            <div class="apc-meta">${(d.stars || 0).toLocaleString()} ⭐ &nbsp;·&nbsp; осталось: ${left} / ${total}</div>
                        </div>
                        <button class="admin-promo-del" title="Удалить" onclick="adminDeletePromo('${escapeHtml(d.code || '')}')">🗑</button>`;
                    listEl.appendChild(item);
                });
            } catch (e) {
                listEl.innerHTML = '<div class="admin-promo-empty">Ошибка загрузки</div>';
                console.error('adminLoadPromos', e);
            }
        }

        // ── Удалить промокод ──
        async function adminDeletePromo(code) {
            if (!confirm(`Удалить промокод «${code}»?`)) return;
            try {
                await ensureBackendAuth();
                await apiRequest('/api/admin/promo/' + encodeURIComponent(code), { method: 'DELETE' });
                adminLoadPromos();
            } catch (e) {
                alert('Ошибка удаления: ' + e.message);
            }
        }

        // ── Начислить звёзды вручную ──
        async function adminGiveStars() {
            const uid   = (document.getElementById('ap-uid')?.value || '').trim();
            const stars = parseInt(document.getElementById('ap-give-stars')?.value) || 0;
            if (!uid)  { adminMsg('ap-give-msg', 'Введите Telegram ID', false); return; }
            if (stars < 1) { adminMsg('ap-give-msg', 'Введите количество звёзд', false); return; }

            const btn = document.getElementById('ap-give-btn');
            btn.disabled = true; btn.textContent = 'Начисляем...';

            try {
                await ensureBackendAuth();
                const result = await apiRequest('/api/admin/give-stars', { body: { uid, stars } });
                adminMsg('ap-give-msg', `✅ Начислено ${stars.toLocaleString()} ⭐ пользователю ${uid}`, true);
                document.getElementById('ap-uid').value = '';
                document.getElementById('ap-give-stars').value = '';
                if (String(uid) === String(myUserId)) {
                    setBalance(result.balance || 0, { persist: false, cache: true });
                }
            } catch (e) {
                console.error('adminGiveStars', e);
                adminMsg('ap-give-msg', 'Ошибка: ' + (e.message || 'неизвестная'), false);
            } finally {
                btn.disabled = false; btn.textContent = 'Начислить';
            }
        }

        // ── Показывать кнопку "Админ" в настройках, если isAdmin() ──
        (function injectAdminBtn() {
            // Вставляется после инициализации myUserId через Telegram
            // Поэтому слушаем момент, когда myUserId уже известен
            const _orig_switchTab = window.switchTab;
            const _tryInject = () => {
                if (!isAdmin()) return;
                const modal = document.getElementById('settingsModal');
                if (!modal || modal.querySelector('#adminSettingsRow')) return;
                const row = document.createElement('div');
                row.id = 'adminSettingsRow';
                row.className = 'modal-row';
                row.style.cssText = 'border-top: 1px solid rgba(255,255,255,0.06); margin-top: 4px;';
                row.innerHTML = '<span style="color:#f5a623;">⚙️ Админ-панель</span><span class="row-arrow">›</span>';
                row.onclick = () => { closeSettings(); openAdmin(); };
                const saveBtn = document.getElementById('settingsSaveBtn');
                if (saveBtn) modal.querySelector('.modal-box').insertBefore(row, saveBtn);
            };
            // Проверяем несколько раз, пока Telegram не отдаст userId
            let _tries = 0;
            const _int = setInterval(() => {
                _tries++;
                if (isAdmin() || _tries > 30) { clearInterval(_int); _tryInject(); }
            }, 300);
        })();
    
