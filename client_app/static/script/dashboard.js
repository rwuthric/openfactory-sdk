document.addEventListener("DOMContentLoaded", function() {
    jsPlumb.ready(function() {
        jsPlumb.setContainer("jsplumb-container")

        const spindleCards = Array.from(document.querySelectorAll('.spindle-card'))
        const gateCards = Array.from(document.querySelectorAll('.gate-card'))
        const dustCollectorCard = document.querySelector('.dust-collector-card')

        function getPrefix(id) {
            const match = id.match(/^([A-Z]+\d+)/)
            return match ? match[1] : id.substring(0, 2)
        }

        spindleCards.forEach(spindle => {
            const spindlePrefix = spindle.id

            gateCards.forEach(gate => {
                const gateId = gate.id.replace('card-', '')
                const gatePrefix = getPrefix(gateId)
                
                if (spindlePrefix === gatePrefix) {
                    jsPlumb.connect({
                        source: spindle.id,
                        target: gate.id,
                        anchors: ["Top", "Top"],
                        endpoint: "Blank",
                        connector: ["Flowchart", { stub: 20, gap: 0, cornerRadius: 5 }],
                        paintStyle: { stroke: "#222", strokeWidth: 15 },
                        overlays: [] 
                    })
                }

                jsPlumb.connect({
                        source: gate.id,
                        target: dustCollectorCard.id,
                        anchors: ["AutoDefault", "AutoDefault"],
                        endpoint: "Blank",
                        connector: ["Flowchart", { stub: 20, gap: 0, cornerRadius: 5 }],
                        paintStyle: { stroke: "#222", strokeWidth: 15 },
                        overlays: [] 
                    })
            })
        })
    })

    initializeDefaultCharts();
})

function saveChartData(dataitemId, data) {
    try {
        const existingData = JSON.parse(localStorage.getItem('chartData') || '{}');
        existingData[dataitemId] = data;
        localStorage.setItem('chartData', JSON.stringify(existingData));
    } catch (e) {
        console.error('Error saving chart data:', e);
    }
}

function getStoredChartData(dataitemId) {
    try {
        const chartData = JSON.parse(localStorage.getItem('chartData') || '{}');
        return chartData[dataitemId] || null;
    } catch (e) {
        console.error('Error retrieving chart data:', e);
        return null;
    }
}

function saveDeviceStates(states) {
    try {
        localStorage.setItem('deviceStates', JSON.stringify(states));
    } catch (e) {
        console.error('Error saving device states:', e);
    }
}

function getStoredDeviceStates() {
    try {
        return JSON.parse(localStorage.getItem('deviceStates') || '{}');
    } catch (e) {
        console.error('Error retrieving device states:', e);
        return {};
    }
}

function initializeDefaultCharts() {
    const toolElements = document.querySelectorAll('[id$="-pie-chart"]');
    const initialData = document.getElementById('initialChartsData')
    console.log(initialData ? initialData.value : 'No initial data element found')

    toolElements.forEach(svg => {
        const dataitemId = svg.id.replace('-pie-chart', '');
        
        let defaultData = getStoredChartData(dataitemId);

        let totalTime = 0;
        
        if (defaultData) {
            Object.keys(defaultData).forEach( state =>{
                totalTime += defaultData[state]
            })

        } else if (initialData && initialData.value) {
            try {
                const parsedData = JSON.parse(initialData.value);
                defaultData = parsedData[dataitemId] || {
                    'OFF': 100,
                    'ON': 1,
                    'UNAVAILABLE': 1
                };
                Object.keys(defaultData).forEach( state =>{
                    totalTime += analyticsData[state]
                })

                saveChartData(dataitemId, defaultData);
            } catch (e) {
                console.error('Error parsing initial data:', e);
                defaultData = {
                    'OFF': 100,
                    'ON': 1,
                    'UNAVAILABLE': 1
                }
                totalTime = 102
            }
        } else {
            defaultData = {
                'OFF': 100,
                'ON': 1,
                'UNAVAILABLE': 1
            }
            totalTime = 102
        }
        
        const labels = ['ON', 'OFF', 'UNAVAILABLE'];
       
        const percentages = [];
        Object.values(defaultData).forEach(value => {
            percentages.push((value / totalTime) * 100);
        });
        
        createPieChart(dataitemId, totalTime, percentages, defaultData, labels);
    });

    const storedStates = getStoredDeviceStates();
    Object.entries(storedStates).forEach(([id, value]) => {
        const valueElem = document.getElementById(id);
        if (valueElem) {
            if (id.includes('Tool')) {
                valueElem.style.color = (value === "ON" ? "#6ed43f" : "red");
                if (valueElem.parentElement) {
                    valueElem.parentElement.style.border = (value === "ON" ? "2px solid #6ed43f" : "2px solid red");
                }
            }
            if (id.includes('Gate')) {
                valueElem.src = (value === "OPEN" ? "../static/icons/blast-gate-open.png" : "../static/icons/blast-gate-closed.png");
            }
        }
    });
}

let device_uuid;
const metaElement = document.querySelector('meta[name="device-uuid"]');
if (metaElement) {
    device_uuid = metaElement.getAttribute('content');
}

if (device_uuid) {
    const wsl_url = `ws://localhost:8000/devices/${device_uuid}/ws`
    const socket = new WebSocket(wsl_url)

    socket.onopen = () => {
        console.log("Socket opened")
    }

    socket.onmessage = (event) => {
        
        const data = JSON.parse(event.data);

        if (data.event === "device_change" && data.data) {
            const id = data.data.id;
            const value = data.data.value;
            
            updateDeviceChart(data.data.id, data.data.durations)
            saveChartData(data.data.id, data.data.durations);
            
            const currentStates = getStoredDeviceStates();
            currentStates[id] = value;
            saveDeviceStates(currentStates);

            const valueElem = document.getElementById(id);
            if (valueElem) {
                
                if (id.includes('Tool'))
                {
                    valueElem.style.color = (value === "ON"? "#6ed43f" : "red")
                    if (valueElem.parentElement) {
                        valueElem.parentElement.style.border = (value === "ON"? "2px solid #6ed43f" : "2px solid red")
                    }
                }

                if (id.includes('Gate')){
                    valueElem.src = (value === "OPEN"? "../static/icons/blast-gate-open.png": "../static/icons/blast-gate-closed.png")
                }
            }
        }

        if (data.event === "connection_established" && data.data_items) {
            saveDeviceStates(data.data_items);
            
            Object.entries(data.data_items).forEach(([id, value]) => {
                const valueElem = document.getElementById(id);
                
                if (valueElem) {
                    if (id.includes('Tool'))
                    {
                        valueElem.style.color = (value === "ON"? "#6ed43f" : "red")
                        if (valueElem.parentElement) {
                            valueElem.parentElement.style.border = (value === "ON"? "2px solid #6ed43f" : "2px solid red")
                        }
                    }

                    if (id.includes('Gate'))
                    {
                        valueElem.src = (value === "OPEN"? "../static/icons/blast-gate-open.png": "../static/icons/blast-gate-closed.png")
                    }
                }
            });
        }
    }

    socket.onclose = () => {
        console.log("Connection closed.")
    }
}

const colors = ["#2F3061", "#ffb800", "#5BC0EB"]

function createPieChart(dataitem_id, total, percentages, data, labels) {
    const svg = document.getElementById(dataitem_id + '-pie-chart');
    
    const radius = 50;
    const centerX = 70;
    const centerY = 70;
    
    if(svg) {
        svg.innerHTML = '';
    
        let currentAngle = -Math.PI / 2;
        
        Object.values(data).forEach((value, index) => {
            const sliceAngle = (value / total) * 2 * Math.PI;
            const endAngle = currentAngle + sliceAngle;
            
            const x1 = centerX + radius * Math.cos(currentAngle);
            const y1 = centerY + radius * Math.sin(currentAngle);
            const x2 = centerX + radius * Math.cos(endAngle);
            const y2 = centerY + radius * Math.sin(endAngle);
            
            const largeArcFlag = sliceAngle > Math.PI ? 1 : 0;

            const pathData = [
                `M ${centerX} ${centerY}`,
                `L ${x1} ${y1}`,
                `A ${radius} ${radius} 0 ${largeArcFlag} 1 ${x2} ${y2}`,
                'Z'
            ].join(' ');
            
            const path = document.createElementNS('http://www.w3.org/2000/svg', 'path');
            path.setAttribute('d', pathData);
            path.setAttribute('fill', colors[index % colors.length]);
            path.setAttribute('class', 'pie-slice');
            path.setAttribute('data-value', `${percentages[index].toFixed(1)}%`);
            path.setAttribute('data-label', labels[index] || `Item ${index + 1}`);
            
            svg.appendChild(path);
            
            currentAngle = endAngle;
        });

        const powerDurationText = document.getElementById(`${dataitem_id}-power-duration`);
        if (powerDurationText) {
            if ((total/60).toFixed(2) > 2) {
                powerDurationText.innerText = `Total powered duration: ${(total/60).toFixed(2)} minutes`;
            } else {
                powerDurationText.innerText = `Total powered duration: NA minutes`;
            }
        }
    }
}

function updateDeviceChart(dataitem_id, analyticsData) {
    const labels = ['ON', 'OFF', 'UNAVAILABLE']
    let totalTime = 0

    const percentages = []
    Object.keys(analyticsData).forEach( state =>{
        totalTime += analyticsData[state]
        percentages.push(analyticsData[state]/totalTime * 100)
    })
    console.log(percentages)
    console.log(totalTime)

    saveChartData(dataitem_id, analyticsData);
    
    createPieChart(dataitem_id, totalTime, percentages, analyticsData, labels);
}