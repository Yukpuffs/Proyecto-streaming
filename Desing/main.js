let prodInterval = null;

document.getElementById('LoadProd').addEventListener('click', () => {
    const contenedor = document.getElementById('espacio1');
    if (!contenedor) return;

    if (contenedor.innerHTML.trim() !== '') {
        contenedor.innerHTML = '';
        if (prodInterval) {
            clearInterval(prodInterval);
            prodInterval = null;
        }
        return;
    }

    const actualizarProductor = () => {
        fetch('/api/eventos')
        .then(response => response.json())
        .then(data => {
            if (data.length === 0) {
                contenedor.innerHTML = `
                    <div class="p-3 text-muted" style="grid-column: span 2;">No hay eventos registrados aún.</div>
                `;
                return;
            }

            const ultimosEventos = data.slice(-4).reverse();

            let htmlContent = '';
            ultimosEventos.forEach(item => {
                const tipo = item.tipo_evento || 'agregar_carrito';
                const esPago = tipo === 'pago_iniciado';
                const total = (item.precio_unitario && item.cantidad) ? (item.precio_unitario * item.cantidad).toLocaleString() : null;
                
                htmlContent += `
                    <div class="card-item rounded p-3 m-3 border-start border-4 shadow-sm cardd">
                        <div class="d-flex justify-content-between align-items-center mb-2">
                            <span class="badge ${esPago ? 'bg-success' : 'bg-primary'}">${tipo.replace('_', ' ')}</span>
                            <small class="text-muted"> ${item.timestamp || ''}</small>
                        </div>
                        <h6 class="mb-1 text-dark"> ${item.usuario}</h6>
                        <p class="mb-1 text-muted small"><strong>Email:</strong> ${item.email || 'N/A'}</p>
                        ${item.producto ? `<div class="p-2 bg-light rounded text-dark small mt-2">📦 <strong>${item.producto}</strong> (x${item.cantidad})${total ? `- $${total}` : ''}</div>` : ''}
                    </div>
                `;
            });

            contenedor.innerHTML = htmlContent;
        })
        .catch(error => console.error('Error en Producer:', error));
    };

    actualizarProductor();
    if (prodInterval) clearInterval(prodInterval);
    prodInterval = setInterval(actualizarProductor, 2000);
});

let consuInterval = null;

document.getElementById('LoadConsu').addEventListener('click', () => {
    const contenedor = document.getElementById('espacio2');
    if (!contenedor) return;

    if (contenedor.innerHTML.trim() !== '') {
        contenedor.innerHTML = '';
        if (consuInterval) {
            clearInterval(consuInterval);
            consuInterval = null;
        }
        return;
    }

    const actualizarConsumidor = () => {
        fetch('/api/notificaciones')
        .then(response => response.json())
        .then(data => {
            if (data.length === 0) {
                contenedor.innerHTML = `
                    <div class="p-3 text-muted" style="grid-column: span 2;">
                        No hay notificaciones de carritos abandonados procesadas aún.
                    </div>`;
                return;
            }

            const ultimasNotificaciones = data.slice(-4).reverse();

            let htmlContent = '';
            ultimasNotificaciones.forEach(item => {
                const total = item.valor_carrito ? item.valor_carrito.toLocaleString() : 0;
                const productos = Array.isArray(item.productos) ? item.productos.join(', ') : item.productos;

                htmlContent += `
                    <div class="card-item rounded p-3 border-start border-4 shadow-s m-3 cardd">
                        <div class="d-flex justify-content-between align-items-center mb-2">
                            <span class="badge bg-danger">Carrito Abandonado</span>
                            <span class="badge bg-success">$${total}</span>
                        </div>
                        <p class="mb-1 text-dark small"><strong>Usuario:</strong> ${item.usuario}</p>
                        <p class="mb-1 text-muted small"><strong>Email:</strong> ${item.email}</p>
                        <p class="mb-2 text-primary small font-italic mensaje">"${item.mensaje}"</p>
                        <div class="p-2 bg-light rounded text-secondary small"> <strong>Productos:</strong> ${productos}</div>
                    </div>
                `;
            });

            contenedor.innerHTML = htmlContent;
        })
        .catch(error => console.error('Error en Consumer:', error));
    };

    actualizarConsumidor();
    if (consuInterval) clearInterval(consuInterval);
    consuInterval = setInterval(actualizarConsumidor, 3000);
});