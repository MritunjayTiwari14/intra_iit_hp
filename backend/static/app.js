const charts = {};
let selectedPatientId = null;

function initCharts() {
    const commonOptions = {
        responsive: true,
        maintainAspectRatio: false,
        animation: { duration: 0 },
        scales: {
            x: { display: false },
            y: { grid: { color: '#1F2937' }, ticks: { color: '#9CA3AF' } }
        },
        plugins: { legend: { display: false } }
    };

    const createChart = (id, color, title) => {
        const ctx = document.getElementById(id).getContext('2d');
        return new Chart(ctx, {
            type: 'line',
            data: { labels: [], datasets: [{ data: [], borderColor: color, tension: 0.4, borderWidth: 2 }] },
            options: { ...commonOptions, plugins: { title: { display: true, text: title, color: '#9CA3AF' }, legend: { display: false } } }
        });
    };

    charts.hr = createChart('chart-hr', '#F87171', 'Heart Rate');
    charts.bp = createChart('chart-bp', '#60A5FA', 'Blood Pressure (SBP)');
    charts.spo2 = createChart('chart-spo2', '#34D399', 'SpO2 (%)');
    charts.rr = createChart('chart-rr', '#FBBF24', 'Resp Rate');
}

function updateCharts(vitals) {
    if (!vitals || vitals.length === 0) return;
    const recent = vitals.slice(-48);
    const labels = recent.map(v => v.timestamp);
    
    charts.hr.data.labels = labels;
    charts.hr.data.datasets[0].data = recent.map(v => v.hr);
    charts.hr.update();

    charts.bp.data.labels = labels;
    charts.bp.data.datasets[0].data = recent.map(v => v.sbp);
    charts.bp.update();

    charts.spo2.data.labels = labels;
    charts.spo2.data.datasets[0].data = recent.map(v => v.spo2);
    charts.spo2.update();

    charts.rr.data.labels = labels;
    charts.rr.data.datasets[0].data = recent.map(v => v.rr);
    charts.rr.update();
}

async function fetchPatientDetail(pid) {
    selectedPatientId = pid;
    document.getElementById('selected-patient-id').textContent = pid;
    
    document.querySelectorAll('#cohort-table tbody tr').forEach(tr => tr.classList.remove('selected'));
    const row = document.getElementById(`row-${pid}`);
    if (row) row.classList.add('selected');

    try {
        const res = await fetch(`/api/vitals/${pid}`);
        const data = await res.json();
        updateCharts(data.vitals);
    } catch(e) { console.error(e); }
}



function renderCohort(cohortData) {
    const tbody = document.querySelector('#cohort-table tbody');
    tbody.innerHTML = '';
    document.getElementById('cohort-count').textContent = cohortData.count;

    const urgencyOrder = { "ESCALATION": 0, "WATCH": 1, "STABLE": 2 };
    const sorted = cohortData.cohort.sort((a,b) => urgencyOrder[a.urgency || "STABLE"] - urgencyOrder[b.urgency || "STABLE"]);

    sorted.forEach(p => {
        const tr = document.createElement('tr');
        tr.id = `row-${p.patient_id}`;
        tr.onclick = () => fetchPatientDetail(p.patient_id);
        if (p.patient_id === selectedPatientId) tr.classList.add('selected');

        const cv = p.current_vitals || {};
        const map = cv.dbp ? Math.round(cv.dbp + (1/3)*(cv.sbp - cv.dbp)) : 0;
        const hr = cv.hr ? Math.round(cv.hr) : '—';
        const bp = cv.sbp ? `${Math.round(cv.sbp)}/${Math.round(cv.dbp)} (${map})` : '—';
        const spo2 = cv.spo2 ? `${Math.round(cv.spo2)}%` : '—';
        const rr = cv.rr ? Math.round(cv.rr) : '—';
        const urg = p.urgency || "STABLE";

        tr.innerHTML = `
            <td><strong>${p.patient_id}</strong></td>
            <td>${p.static_context?.bed || '—'}</td>
            <td><span class="urgency-${urg}">${urg}</span></td>
            <td><strong>${p.mews_total || 0}</strong></td>
            <td>${hr}</td>
            <td>${bp}</td>
            <td>${spo2}</td>
            <td>${rr}</td>
        `;
        tbody.appendChild(tr);
    });

    if (!selectedPatientId && sorted.length > 0) {
        fetchPatientDetail(sorted[0].patient_id);
    }
}

function renderEscalations(escalations) {
    const active = escalations.active || [];
    document.getElementById('escalation-count').textContent = escalations.active_count;
    
    if (!selectedPatientId) return;
    
    const container = document.getElementById('escalation-cards-container');
    container.innerHTML = '';
    
    const ptEscalations = active.filter(e => e.patient_id === selectedPatientId && e.status === 'PENDING');
    
    ptEscalations.forEach(esc => {
        const div = document.createElement('div');
        div.className = 'escalation-card';
        div.innerHTML = `
            <h3>🚨 Escalation: ${esc.escalation_type}</h3>
            <div class="sbar-section"><strong>Status:</strong> ${esc.status}</div>
            <div class="sbar-section"><strong>Reasoning:</strong><br>${esc.Reasoning.replace(/\n/g, '<br>')}</div>
            <div class="sbar-section"><strong>Action:</strong> ${esc.Recommended_Action}</div>
            <div style="margin-top: 1rem; display: flex; gap: 0.5rem;">
                <button onclick="handleAction('${esc.escalation_id}', 'ACCEPT')" style="background: var(--stable-bg); padding: 0.5rem 1rem; border-radius: 4px; border:none; color:white; cursor:pointer; font-weight: bold;">Accept & Clear</button>
            </div>
        `;
        container.appendChild(div);
    });
}

function connectWebSocket() {
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const wsUrl = `${protocol}//${window.location.host}/ws`;
    const ws = new WebSocket(wsUrl);

    ws.onmessage = function(event) {
        const msg = JSON.parse(event.data);
        if (msg.type === 'cohort_update') {
            renderCohort(msg.data);
            if (selectedPatientId) fetchPatientDetail(selectedPatientId);
        } else if (msg.type === 'escalations_update') {
            renderEscalations(msg.data);
        } else if (msg.type === 'ping') {
            // Heartbeat
        }
    };
    
    ws.onclose = function() {
        console.log('WebSocket closed, reconnecting in 2s...');
        setTimeout(connectWebSocket, 2000);
    };
}

async function handleAction(escId, action) {
    try {
        const res = await fetch('/api/clinician-action', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ 
                escalation_id: escId, 
                action: action, 
                clinician_id: "dr_m_smith", 
                dismiss_reason: "Cleared by simulation UI" 
            })
        });
        if (!res.ok) throw new Error('Action failed');
        
        // No redirect needed, UI will auto-update via websocket
    } catch(e) {
        console.error(e);
        alert('Failed to submit action');
    }
}

initCharts();
connectWebSocket();
