/* ═══════════════════════════════════════════════════════════════════════════
   Channel Relevance UI – Client Side
   ═══════════════════════════════════════════════════════════════════════════ */

// ── State ──────────────────────────────────────────────────────────────────
const state = {
    lang: "es",
    page: 1,
    pageSize: 50,
    totalPages: 1,
    sortBy: "hit_videos_count",
    sortOrder: "desc",
    channels: [],
    // Modal
    modalChannelUrl: null,
    modalTags: [],
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

function getFilterParams() {
    const p = new URLSearchParams();
    p.set("lang", state.lang);
    p.set("page", state.page);
    p.set("page_size", state.pageSize);
    p.set("sort_by", state.sortBy);
    p.set("sort_order", state.sortOrder);

    const fields = {
        fMinViewsIndividual: "min_views_individual",
        fMaxViewsIndividual: "max_views_individual",
        fMinVideosTotal: "min_videos_total",
        fMaxVideosTotal: "max_videos_total",
        fMinHitsCount: "min_hits_count",
        fMinAvgViews: "min_avg_views",
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

    const verified = $("#fIsVerified").value;
    if (verified !== "") p.set("is_verified", verified);

    const rel = $("#fRelevance").value;
    if (rel && rel !== "all") p.set("relevance_filter", rel);

    return p;
}

// ── Fetch Channels ─────────────────────────────────────────────────────────
async function fetchChannels() {
    tbody.innerHTML = `<tr><td colspan="9" class="loading"><div class="spinner"></div></td></tr>`;

    const params = getFilterParams();
    try {
        const res = await fetch(`/api/channels?${params}`);
        const data = await res.json();

        state.channels = data.channels;
        state.totalPages = data.total_pages;
        state.page = data.page;

        renderTable();
        renderPagination();
    } catch (err) {
        tbody.innerHTML = `<tr><td colspan="9" class="empty-state"><div class="icon">⚠️</div>Error loading channels</td></tr>`;
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
        tbody.innerHTML = `<tr><td colspan="9" class="empty-state"><div class="icon">📭</div>No channels match your filters</td></tr>`;
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
            ? ch.tags.map(t => `<span class="tag-chip" style="margin-left:4px;font-size:.7rem">${t}</span>`).join("")
            : "";

        return `<tr data-idx="${i}">
            <td>
                <a class="channel-link" href="${ch.channel_url}" target="_blank" rel="noopener">${ch.channel_name || "Unknown"}</a>${verified}
            </td>
            <td class="num">${fmt(ch.subscriber_count)}</td>
            <td class="num">${fmt(ch.total_videos_tracked)}</td>
            <td class="num">${fmt(ch.hit_videos_count)}</td>
            <td class="num">${fmtAvg(ch.avg_views_on_channel)}</td>
            <td class="num">${fmt(ch.max_views_on_channel)}</td>
            <td class="num date-cell">${fmtDate(ch.last_upload_date)}</td>
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
}

// ── Render Pagination ──────────────────────────────────────────────────────
function renderPagination() {
    pageInfo.textContent = `Page ${state.page} of ${state.totalPages}`;
    btnPrev.disabled = state.page <= 1;
    btnNext.disabled = state.page >= state.totalPages;
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
        state.page = 1;
        $$(".lang-toggle button").forEach(b => b.classList.remove("active"));
        btn.classList.add("active");
        fetchChannels();
        fetchStats();
    });
});

// Apply / Reset
btnApply.addEventListener("click", () => {
    state.page = 1;
    fetchChannels();
});

btnReset.addEventListener("click", () => {
    $("#fMinViewsIndividual").value = "10000";
    $("#fMaxViewsIndividual").value = "";
    $("#fMinVideosTotal").value = "3";
    $("#fMaxVideosTotal").value = "";
    $("#fMinHitsCount").value = "1";
    $("#fMinAvgViews").value = "5000";
    $("#fMinSubscribers").value = "";
    $("#fMaxSubscribers").value = "";
    $("#fLastUploadedAfter").value = "";
    $("#fLastUploadedBefore").value = "";
    $("#fIsVerified").value = "";
    $("#fChannelName").value = "";
    $("#fRelevance").value = "all";
    $("#fTagFilter").value = "";
    state.page = 1;
    fetchChannels();
});

// Pagination
btnPrev.addEventListener("click", () => { if (state.page > 1) { state.page--; fetchChannels(); } });
btnNext.addEventListener("click", () => { if (state.page < state.totalPages) { state.page++; fetchChannels(); } });

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
        state.page = 1;
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
});

// Modal
btnModalCancel.addEventListener("click", closeModal);
modalOverlay.addEventListener("click", (e) => { if (e.target === modalOverlay) closeModal(); });

btnModalSave.addEventListener("click", saveModal);

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
        if (e.key === "Enter") { state.page = 1; fetchChannels(); }
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
updateSortArrows();
fetchChannels();
fetchStats();
