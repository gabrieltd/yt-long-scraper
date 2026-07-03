/* ═══════════════════════════════════════════════════════════════════════════
   Channel Relevance UI – Client Side
   ═══════════════════════════════════════════════════════════════════════════ */

// ── State ──────────────────────────────────────────────────────────────────
const state = {
    lang: "es",
    page: 1,
    pageSize: 50,
    currentCursor: null,
    cursorHistory: [],
    hasNext: false,
    nextCursor: null,
    sortBy: "channel_name",
    sortOrder: "desc",
    channels: [],
    // Modal
    modalChannelUrl: null,
    modalTags: [],
    // Bulk Selection
    selectedUrls: new Set(),
    drawerChannelUrl: null,
    drawerWidth: null,
};

// ── DOM References ─────────────────────────────────────────────────────────
const $ = (sel) => document.querySelector(sel);
const $$ = (sel) => document.querySelectorAll(sel);

const tbody = $("#channelTableBody");
const pageInfo = $("#pageInfo");
const btnPrev = $("#btnPrev");
const btnNext = $("#btnNext");
const btnApply = $("#btnApply");
const btnReset = $("#btnReset");
const modalOverlay = $("#modalOverlay");
const modalTitle = $("#modalTitle");
const modalNotes = $("#modalNotes");
const tagsWrapper = $("#tagsWrapper");
const tagInput = $("#tagInput");
const btnModalCancel = $("#btnModalCancel");
const btnModalSave = $("#btnModalSave");

// Added for bulk actions
const selectAllCheckbox = $("#selectAllCheckbox");
const bulkActions = $("#bulkActions");
const selectedCount = $("#selectedCount");
const btnBulkRelevant = $("#btnBulkRelevant");
const btnBulkNotRelevant = $("#btnBulkNotRelevant");
const channelDrawer = $("#channelDrawer");
const drawerBackdrop = $("#drawerBackdrop");
const drawerContent = $("#drawerContent");
const drawerResizeHandle = $("#drawerResizeHandle");

// ── Helpers ────────────────────────────────────────────────────────────────
function fmt(n) {
    if (n == null) return "–";
    return Number(n).toLocaleString("en-US");
}

function fmtAvg(n) {
    if (n == null) return "–";
    return Number(n).toLocaleString("en-US", { maximumFractionDigits: 0 });
}

function fmtDate(yyyymmdd) {
    if (!yyyymmdd || yyyymmdd.length !== 8) return "–";
    const y = yyyymmdd.slice(0, 4);
    const m = yyyymmdd.slice(4, 6);
    const d = yyyymmdd.slice(6, 8);
    return `${y}-${m}-${d}`;
}

function escapeHtml(value) {
    return String(value ?? "").replace(/[&<>'"]/g, char => ({
        "&": "&amp;", "<": "&lt;", ">": "&gt;", "'": "&#39;", '"': "&quot;",
    })[char]);
}

function avatarMarkup(url, name, className = "channel-avatar") {
    if (url) {
        return `<img class="${className}" src="${escapeHtml(url)}" alt="" />`;
    }
    return `<span class="${className} avatar-fallback" aria-hidden="true">${escapeHtml((name || "?").slice(0, 1).toUpperCase())}</span>`;
}

function channelVideosUrl(channelUrl) {
    const base = String(channelUrl || "").replace(/\/+$/, "");
    return base.endsWith("/videos") ? base : `${base}/videos`;
}

function getFilterParams() {
    const p = new URLSearchParams();
    p.set("lang", state.lang);
    p.set("page_size", state.pageSize);
    p.set("sort_by", state.sortBy);
    p.set("sort_order", state.sortOrder);
    if (state.currentCursor) p.set("cursor", state.currentCursor);

    const fields = {
        fMinSubscribers: "min_subscribers",
        fMaxSubscribers: "max_subscribers",
        fChannelName: "channel_name_search",
        fTagFilter: "tag_filter",
    };

    for (const [id, key] of Object.entries(fields)) {
        const v = $(`#${id}`).value.trim();
        if (v !== "") p.set(key, v);
    }

    // Date filters: convert YYYY-MM-DD (input[type=date]) → YYYYMMDD
    const afterVal = $("#fLastUploadedAfter").value;
    if (afterVal) p.set("last_uploaded_after", afterVal.replace(/-/g, ""));
    const beforeVal = $("#fLastUploadedBefore").value;
    if (beforeVal) p.set("last_uploaded_before", beforeVal.replace(/-/g, ""));

    // First upload date filters
    const firstAfterVal = $("#fFirstUploadedAfter").value;
    if (firstAfterVal) p.set("first_uploaded_after", firstAfterVal.replace(/-/g, ""));
    const firstBeforeVal = $("#fFirstUploadedBefore").value;
    if (firstBeforeVal) p.set("first_uploaded_before", firstBeforeVal.replace(/-/g, ""));

    const verified = $("#fIsVerified").value;
    if (verified !== "") p.set("is_verified", verified);

    const rel = $("#fRelevance").value;
    if (rel && rel !== "all") p.set("relevance_filter", rel);

    return p;
}

// ── Fetch Channels ─────────────────────────────────────────────────────────
async function fetchChannels() {
    tbody.innerHTML = `<tr><td colspan="7" class="loading"><div class="spinner"></div></td></tr>`;

    const params = getFilterParams();
    try {
        const res = await fetch(`/api/channels?${params}`);
        if (!res.ok) throw new Error("Unable to load channels");
        const data = await res.json();

        state.channels = data.channels;
        state.hasNext = data.has_next;
        state.nextCursor = data.next_cursor;

        renderTable();
        renderPagination();
    } catch (err) {
        tbody.innerHTML = `<tr><td colspan="7" class="empty-state"><div class="icon">⚠️</div>Error loading channels</td></tr>`;
        console.error(err);
    }
}

// ── Fetch Stats ────────────────────────────────────────────────────────────
async function fetchStats() {
    try {
        const res = await fetch(`/api/stats/${state.lang}`);
        const data = await res.json();
        $("#statTotal").textContent = fmt(data.total_channels);
        $("#statRelevant").textContent = fmt(data.relevant);
        $("#statNotRelevant").textContent = fmt(data.not_relevant);
        $("#statUnmarked").textContent = fmt(data.unmarked);
    } catch (err) {
        console.error("Stats error:", err);
    }
}

// ── Render Table ───────────────────────────────────────────────────────────
function renderTable() {
    if (state.channels.length === 0) {
        tbody.innerHTML = `<tr><td colspan="7" class="empty-state"><div class="icon">📭</div>No channels match your filters</td></tr>`;
        updateBulkActionsUI();
        return;
    }

    tbody.innerHTML = state.channels.map((ch, i) => {
        const relClass = ch.is_relevant === true ? "relevant" :
                         ch.is_relevant === false ? "not-relevant" : "unmarked";
        const relLabel = ch.is_relevant === true ? "Relevant" :
                         ch.is_relevant === false ? "Not Relevant" : "Unmarked";

        const yesActive = ch.is_relevant === true ? " active-yes" : "";
        const noActive  = ch.is_relevant === false ? " active-no" : "";

        const verified = ch.is_verified ? `<span class="verified-badge" title="Verified">✓</span>` : "";

        const tagsHtml = ch.tags && ch.tags.length > 0
            ? ch.tags.map(t => `<span class="tag-chip" style="margin-left:4px;font-size:.7rem">${escapeHtml(t)}</span>`).join("")
            : "";

        const isChecked = state.selectedUrls.has(ch.channel_url) ? "checked" : "";

        return `<tr data-idx="${i}">
            <td style="text-align: center;">
                <input type="checkbox" class="row-checkbox" data-url="${encodeURIComponent(ch.channel_url)}" ${isChecked} />
            </td>
            <td>
                <span class="channel-cell">
                    <a class="channel-thumbnail-link" href="${escapeHtml(channelVideosUrl(ch.channel_url))}" target="_blank" rel="noopener" title="Open channel videos">
                        ${avatarMarkup(ch.avatar_url, ch.channel_name)}
                    </a>
                    <button class="channel-trigger" data-action="details" data-url="${encodeURIComponent(ch.channel_url)}" title="Open channel details">
                        <span class="channel-link">${escapeHtml(ch.channel_name || "Unknown")}</span>${verified}
                    </button>
                </span>
            </td>
            <td class="num">${fmt(ch.subscriber_count)}</td>
            <td class="num date-cell">${fmtDate(ch.last_upload_date)}</td>
            <td class="num date-cell">${fmtDate(ch.first_upload_date)}</td>
            <td><span class="relevance-badge ${relClass}">${relLabel}</span>${tagsHtml}</td>
            <td>
                <div class="action-btns">
                    <button class="btn-yes${yesActive}" title="Mark Relevant" data-url="${encodeURIComponent(ch.channel_url)}" data-action="yes">👍</button>
                    <button class="btn-no${noActive}" title="Mark Not Relevant" data-url="${encodeURIComponent(ch.channel_url)}" data-action="no">👎</button>
                    <button class="btn-edit" title="Edit notes & tags" data-url="${encodeURIComponent(ch.channel_url)}" data-action="edit">✏️</button>
                </div>
            </td>
        </tr>`;
    }).join("");

    updateBulkActionsUI();
}

function closeDrawer() {
    channelDrawer.classList.remove("open");
    drawerBackdrop.classList.remove("open");
    channelDrawer.setAttribute("aria-hidden", "true");
    state.drawerChannelUrl = null;
}

function resetChannelPagination() {
    state.page = 1;
    state.currentCursor = null;
    state.cursorHistory = [];
    state.hasNext = false;
    state.nextCursor = null;
}

const DRAWER_MIN_WIDTH = 400;
const DRAWER_MAX_WIDTH = 720;
const DRAWER_WIDTH_STORAGE_KEY = "channelDrawerWidth";

function getDrawerMaxWidth() {
    return Math.min(DRAWER_MAX_WIDTH, Math.floor(window.innerWidth * 0.9));
}

function setDrawerWidth(width, { persist = false } = {}) {
    if (window.innerWidth <= 768) {
        channelDrawer.style.removeProperty("width");
        return;
    }

    const maxWidth = getDrawerMaxWidth();
    const normalized = Math.max(Math.min(Number(width) || 500, maxWidth), Math.min(DRAWER_MIN_WIDTH, maxWidth));
    state.drawerWidth = normalized;
    channelDrawer.style.width = `${normalized}px`;
    if (persist) localStorage.setItem(DRAWER_WIDTH_STORAGE_KEY, String(normalized));
}

function restoreDrawerWidth() {
    const saved = Number(localStorage.getItem(DRAWER_WIDTH_STORAGE_KEY));
    setDrawerWidth(Number.isFinite(saved) && saved > 0 ? saved : 500);
}

function startDrawerResize(event) {
    if (window.innerWidth <= 768 || event.button !== 0) return;
    event.preventDefault();
    const startX = event.clientX;
    const startWidth = channelDrawer.getBoundingClientRect().width;

    document.body.classList.add("drawer-resizing");
    const move = (moveEvent) => {
        setDrawerWidth(startWidth + startX - moveEvent.clientX);
    };
    const stop = () => {
        document.body.classList.remove("drawer-resizing");
        if (state.drawerWidth) localStorage.setItem(DRAWER_WIDTH_STORAGE_KEY, String(state.drawerWidth));
        window.removeEventListener("pointermove", move);
        window.removeEventListener("pointerup", stop);
    };

    window.addEventListener("pointermove", move);
    window.addEventListener("pointerup", stop, { once: true });
}

function detailMetric(label, value) {
    return `<div class="detail-metric"><span>${escapeHtml(label)}</span><strong>${escapeHtml(value)}</strong></div>`;
}

function renderDrawer(details) {
    const ch = details.channel;
    const relClass = ch.is_relevant === true ? "relevant" : ch.is_relevant === false ? "not-relevant" : "unmarked";
    const relLabel = ch.is_relevant === true ? "Relevant" : ch.is_relevant === false ? "Not Relevant" : "Unmarked";
    const banner = ch.banner_url
        ? `<img class="drawer-banner" src="${escapeHtml(ch.banner_url)}" alt="" />`
        : `<div class="drawer-banner drawer-banner-empty"></div>`;
    const rawTags = ch.channel_tags.length
        ? ch.channel_tags.map(tag => `<span class="drawer-tag">${escapeHtml(tag)}</span>`).join("")
        : `<span class="drawer-muted">No channel tags</span>`;
    const reviewTags = ch.tags.length
        ? ch.tags.map(tag => `<span class="drawer-tag review-tag">${escapeHtml(tag)}</span>`).join("")
        : "";
    const encodedUrl = encodeURIComponent(ch.channel_url);

    drawerContent.innerHTML = `
        <div class="drawer-topbar">
            <strong>${escapeHtml(ch.channel_name || "Channel")}</strong>
            <button class="drawer-close" type="button" data-drawer-action="close" title="Close details" aria-label="Close details">×</button>
        </div>
        ${banner}
        <section class="drawer-profile">
            ${avatarMarkup(ch.avatar_url, ch.channel_name, "drawer-avatar")}
            <div>
                <div class="drawer-name">${escapeHtml(ch.channel_name || "Unknown")}${ch.is_verified ? `<span class="verified-badge" title="Verified">✓</span>` : ""}</div>
                <div class="drawer-handle">${escapeHtml(ch.uploader_id || "")}</div>
                <div class="drawer-subscribers">${fmt(ch.subscriber_count)} subscribers</div>
            </div>
        </section>
        <div class="drawer-actions">
            <a class="drawer-channel-link" href="${escapeHtml(ch.uploader_url || ch.channel_url)}" target="_blank" rel="noopener">Open on YouTube</a>
            <button class="drawer-action-button" type="button" data-drawer-action="edit" data-url="${encodedUrl}">Edit review</button>
        </div>
        ${ch.channel_description ? `<p class="drawer-description">${escapeHtml(ch.channel_description).replace(/\n/g, "<br>")}</p>` : ""}
        <section class="drawer-section">
            <h3>Tags</h3>
            <div class="drawer-tags">${rawTags}${reviewTags}</div>
        </section>
        <section class="drawer-section">
            <div class="drawer-section-heading"><h3>Channel info</h3><span class="relevance-badge ${relClass}">${relLabel}</span></div>
            <div class="detail-metrics">
                ${detailMetric("Subscribers", fmt(ch.subscriber_count))}
                ${detailMetric("Last upload", fmtDate(ch.last_upload_date))}
                ${detailMetric("First upload", fmtDate(ch.first_upload_date))}
            </div>
        </section>
    `;
}

async function openDetails(encodedUrl) {
    const channelUrl = decodeURIComponent(encodedUrl);
    state.drawerChannelUrl = channelUrl;
    channelDrawer.classList.add("open");
    drawerBackdrop.classList.add("open");
    channelDrawer.setAttribute("aria-hidden", "false");
    drawerContent.innerHTML = `<div class="drawer-loading"><div class="spinner"></div><span>Loading channel details...</span></div>`;

    const params = new URLSearchParams({ lang: state.lang });

    try {
        const res = await fetch(`/api/channels/${encodedUrl}/details?${params}`);
        if (!res.ok) throw new Error("Unable to load channel details");
        const details = await res.json();
        if (state.drawerChannelUrl === channelUrl) renderDrawer(details);
    } catch (err) {
        drawerContent.innerHTML = `<div class="drawer-loading"><span>Unable to load channel details.</span></div>`;
        console.error(err);
    }
}

function updateBulkActionsUI() {
    const total = state.selectedUrls.size;
    
    if (total > 0) {
        bulkActions.style.display = "flex";
        selectedCount.textContent = `${total} selected`;
    } else {
        bulkActions.style.display = "none";
    }

    if (state.channels.length > 0) {
        const allOnPageSelected = state.channels.every(ch => state.selectedUrls.has(ch.channel_url));
        selectAllCheckbox.checked = allOnPageSelected && total > 0;
        selectAllCheckbox.indeterminate = total > 0 && !allOnPageSelected;
    } else {
        selectAllCheckbox.checked = false;
        selectAllCheckbox.indeterminate = false;
    }
}

// ── Render Pagination ──────────────────────────────────────────────────────
function renderPagination() {
    pageInfo.textContent = `Page ${state.page}`;
    btnPrev.disabled = state.page <= 1;
    btnNext.disabled = !state.hasNext;
}

// ── Mark Relevance ─────────────────────────────────────────────────────────
async function markRelevance(encodedUrl, isRelevant) {
    const channelUrl = decodeURIComponent(encodedUrl);
    const ch = state.channels.find(c => c.channel_url === channelUrl);

    // Toggle: if already set to this value, unset (null)
    const newValue = (ch && ch.is_relevant === isRelevant) ? null : isRelevant;

    try {
        await fetch(`/api/channels/${encodedUrl}/relevance`, {
            method: "PATCH",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
                lang: state.lang,
                is_relevant: newValue,
                notes: ch ? ch.notes : null,
                tags: ch ? ch.tags : [],
            }),
        });

        // Update local state
        if (ch) ch.is_relevant = newValue;
        renderTable();
        fetchStats();
    } catch (err) {
        console.error("Error marking relevance:", err);
    }
}

// ── Modal ──────────────────────────────────────────────────────────────────
function openModal(encodedUrl) {
    const channelUrl = decodeURIComponent(encodedUrl);
    const ch = state.channels.find(c => c.channel_url === channelUrl);
    if (!ch) return;

    state.modalChannelUrl = channelUrl;
    state.modalTags = ch.tags ? [...ch.tags] : [];

    modalTitle.textContent = `Edit: ${ch.channel_name || channelUrl}`;
    modalNotes.value = ch.notes || "";
    renderModalTags();
    modalOverlay.classList.add("open");
}

function closeModal() {
    modalOverlay.classList.remove("open");
    state.modalChannelUrl = null;
    state.modalTags = [];
}

function renderModalTags() {
    // Remove existing chips
    tagsWrapper.querySelectorAll(".tag-chip").forEach(el => el.remove());
    // Add chips before the input
    state.modalTags.forEach((tag, i) => {
        const chip = document.createElement("span");
        chip.className = "tag-chip";
        chip.innerHTML = `${tag} <span class="remove-tag" data-tag-idx="${i}">&times;</span>`;
        tagsWrapper.insertBefore(chip, tagInput);
    });
}

async function saveModal() {
    if (!state.modalChannelUrl) return;
    const ch = state.channels.find(c => c.channel_url === state.modalChannelUrl);
    const encodedUrl = encodeURIComponent(state.modalChannelUrl);

    try {
        await fetch(`/api/channels/${encodedUrl}/relevance`, {
            method: "PATCH",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
                lang: state.lang,
                is_relevant: ch ? ch.is_relevant : null,
                notes: modalNotes.value.trim() || null,
                tags: state.modalTags,
            }),
        });

        // Update local state
        if (ch) {
            ch.notes = modalNotes.value.trim() || null;
            ch.tags = [...state.modalTags];
        }
        renderTable();
        closeModal();
    } catch (err) {
        console.error("Error saving:", err);
    }
}

// ── Sort ───────────────────────────────────────────────────────────────────
function updateSortArrows() {
    $$("thead th").forEach(th => {
        const arrow = th.querySelector(".sort-arrow");
        if (!arrow) return;
        if (th.dataset.sort === state.sortBy) {
            arrow.textContent = state.sortOrder === "asc" ? "▲" : "▼";
        } else {
            arrow.textContent = "";
        }
    });
}

// ── Event Listeners ────────────────────────────────────────────────────────

// Language toggle
$$(".lang-toggle button").forEach(btn => {
    btn.addEventListener("click", () => {
        state.lang = btn.dataset.lang;
        resetChannelPagination();
        closeDrawer();
        $$(".lang-toggle button").forEach(b => b.classList.remove("active"));
        btn.classList.add("active");
        fetchChannels();
        fetchStats();
    });
});

// Apply / Reset
btnApply.addEventListener("click", () => {
    resetChannelPagination();
    fetchChannels();
});

btnReset.addEventListener("click", () => {
    $("#fMinSubscribers").value = "";
    $("#fMaxSubscribers").value = "";
    $("#fLastUploadedAfter").value = "";
    $("#fLastUploadedBefore").value = "";
    $("#fFirstUploadedAfter").value = "";
    $("#fFirstUploadedBefore").value = "";
    $("#fIsVerified").value = "";
    $("#fChannelName").value = "";
    $("#fRelevance").value = "all";
    $("#fTagFilter").value = "";
    resetChannelPagination();
    fetchChannels();
});

// Pagination
btnPrev.addEventListener("click", () => {
    if (state.page <= 1) return;
    state.currentCursor = state.cursorHistory.pop() || null;
    state.page--;
    fetchChannels();
});
btnNext.addEventListener("click", () => {
    if (!state.hasNext || !state.nextCursor) return;
    state.cursorHistory.push(state.currentCursor);
    state.currentCursor = state.nextCursor;
    state.page++;
    fetchChannels();
});

// Sortable headers
$$("thead th[data-sort]").forEach(th => {
    th.addEventListener("click", () => {
        const col = th.dataset.sort;
        if (state.sortBy === col) {
            state.sortOrder = state.sortOrder === "desc" ? "asc" : "desc";
        } else {
            state.sortBy = col;
            state.sortOrder = "desc";
        }
        updateSortArrows();
        resetChannelPagination();
        fetchChannels();
    });
});

// Table action buttons (delegated)
tbody.addEventListener("click", (e) => {
    const btn = e.target.closest("button");
    if (!btn) return;
    const action = btn.dataset.action;
    const url = btn.dataset.url;
    if (!action || !url) return;

    if (action === "yes") markRelevance(url, true);
    else if (action === "no") markRelevance(url, false);
    else if (action === "edit") openModal(url);
    else if (action === "details") openDetails(url);
});

drawerContent.addEventListener("click", (e) => {
    const btn = e.target.closest("button[data-drawer-action]");
    if (!btn) return;
    const action = btn.dataset.drawerAction;
    if (action === "close") closeDrawer();
    else if (action === "edit") {
        closeDrawer();
        openModal(btn.dataset.url);
    }
});

drawerBackdrop.addEventListener("click", closeDrawer);
drawerResizeHandle.addEventListener("pointerdown", startDrawerResize);
window.addEventListener("resize", () => {
    if (window.innerWidth <= 768) {
        channelDrawer.style.removeProperty("width");
    } else {
        setDrawerWidth(state.drawerWidth || Number(localStorage.getItem(DRAWER_WIDTH_STORAGE_KEY)) || 500);
    }
});
document.addEventListener("keydown", (e) => {
    if (e.key === "Escape" && channelDrawer.classList.contains("open")) closeDrawer();
});

// Modal
btnModalCancel.addEventListener("click", closeModal);
modalOverlay.addEventListener("click", (e) => { if (e.target === modalOverlay) closeModal(); });

btnModalSave.addEventListener("click", saveModal);

// Bulk Selection Event Listeners
selectAllCheckbox.addEventListener("change", (e) => {
    const isChecked = e.target.checked;
    state.channels.forEach(ch => {
        if (isChecked) {
            state.selectedUrls.add(ch.channel_url);
        } else {
            state.selectedUrls.delete(ch.channel_url);
        }
    });
    
    $$(".row-checkbox").forEach(cb => cb.checked = isChecked);
    updateBulkActionsUI();
});

tbody.addEventListener("change", (e) => {
    if (e.target.classList.contains("row-checkbox")) {
        const url = decodeURIComponent(e.target.dataset.url);
        if (e.target.checked) {
            state.selectedUrls.add(url);
        } else {
            state.selectedUrls.delete(url);
        }
        updateBulkActionsUI();
    }
});

async function markBulkRelevance(isRelevant) {
    if (state.selectedUrls.size === 0) return;
    const urls = Array.from(state.selectedUrls);
    
    try {
        await fetch(`/api/channels/bulk-relevance`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
                lang: state.lang,
                channel_urls: urls,
                is_relevant: isRelevant
            }),
        });
        
        state.channels.forEach(ch => {
            if (state.selectedUrls.has(ch.channel_url)) {
                ch.is_relevant = isRelevant;
            }
        });
        
        state.selectedUrls.clear();
        renderTable();
        fetchStats();
    } catch (err) {
        console.error("Error bulk marking relevance:", err);
    }
}

btnBulkRelevant.addEventListener("click", () => markBulkRelevance(true));
btnBulkNotRelevant.addEventListener("click", () => markBulkRelevance(false));

// Tag input
tagInput.addEventListener("keydown", (e) => {
    if (e.key === "Enter") {
        e.preventDefault();
        const val = tagInput.value.trim().toLowerCase().replace(/\s+/g, "_");
        if (val && !state.modalTags.includes(val)) {
            state.modalTags.push(val);
            renderModalTags();
        }
        tagInput.value = "";
    }
});

// Remove tag chip
tagsWrapper.addEventListener("click", (e) => {
    const rm = e.target.closest(".remove-tag");
    if (!rm) return;
    const idx = parseInt(rm.dataset.tagIdx, 10);
    state.modalTags.splice(idx, 1);
    renderModalTags();
});

// Enter key on filter inputs triggers apply
$$(".filter-group input, .filter-group select").forEach(el => {
    el.addEventListener("keydown", (e) => {
        if (e.key === "Enter") { resetChannelPagination(); fetchChannels(); }
    });
});

// ── Sidebar toggle ─────────────────────────────────────────────────────────
const btnToggleSidebar = $("#btnToggleSidebar");
const mainLayout = $("#mainLayout");

function applySidebarState(active) {
    if (active) {
        mainLayout.classList.add("sidebar-mode");
        btnToggleSidebar.classList.add("active");
    } else {
        mainLayout.classList.remove("sidebar-mode");
        btnToggleSidebar.classList.remove("active");
    }
}

// Restore sidebar state from localStorage
if (localStorage.getItem("sidebarMode") === "true") {
    applySidebarState(true);
}

btnToggleSidebar.addEventListener("click", () => {
    const isActive = mainLayout.classList.toggle("sidebar-mode");
    btnToggleSidebar.classList.toggle("active", isActive);
    localStorage.setItem("sidebarMode", isActive);
});

// ── Init ───────────────────────────────────────────────────────────────────
restoreDrawerWidth();
updateSortArrows();
fetchChannels();
fetchStats();
