/* Hacer análisis de sorteos con una figuras haciendo uso de funciones almacenamadas en mongodb */

const MongoClient = require('mongodb').MongoClient;
const assert = require('assert');

// Connection URL
const url = 'mongodb://localhost:27017';

// Database Name
const dbName = 'bingo';

let db;

(async function () {
    const client = new MongoClient(url);

    try {
        await client.connect();
        console.log("* * *  Connected correctly to server");

        db = client.db(dbName);

        let p;

        let start = new Date();

        //Limpiar tachados y balotas de sorteo anterior
        // p = await limpiaJuego();
        p = await db.eval("limpiaJuego()");

        //Eleccion figura
        // p = await agregarFigura(32, 1);
        p = await db.eval("agregarFigura(32, 1)");

        //venta modulo o lote de cartones
        // p = await agregarLote(30000);
        p = await db.eval("agregarLote(30000)");

        //Nueva balota
        let balota_juegoObj;
        let balotasAr = [];
        let posNewBalota;

        //arreglo
        for (let i = 0; i < 75; i++) {
            balotasAr[i] = i + 1;
        }

        //Siguiente balota

        console.log("* * * inicio sorteo !");
        for (let i = 0; i < 75; i++) {

            //posicion aleatoria
            posNewBalota = parseInt(Math.random() * (balotasAr.length - 1));

            //balota segun posicion anterior
            let balota = balotasAr[posNewBalota];

            console.log("* * * balota: ", balota);

            //siguiente balota
            balota_juegoObj = await siguienteBalota(1, i, balota);

            //elimino balota actual
            balotasAr.splice(posNewBalota, 1);

            //console.log("* * * balotasAr.length,balotasAr: ", balotasAr.length, balotasAr);
            if (balota_juegoObj.ganadores != null) {
                break;
            }

        }

        console.log("* * * terminó de sacar todas las balotas !");

        let end = new Date() - start;
        console.info('* * * Execution time: %d min', (end / 60000))

    } catch (err) {

        console.log(err.stack);
    }

    // Close connection
    client.close();
})();


/**
 *    Funcion usada para limpiar los datos del calculo de un gandor
 **/
async function limpiaJuego() {
    let r;
    r = await db.collection('balota_sorteo').deleteMany({});
    r = await db.collection('figura_juego').deleteMany({});
    r = await db.collection('ganadores').deleteMany({});
    r = await db.collection('punteros').deleteMany({});

    r = await db.collection('tachados').deleteMany({});
    r = await db.collection('vendidos').deleteMany({});
    /*r = await db.collection('figura_juego').updateMany({}, {
        $set: {
            ganado: false
        }
    });
}

/**
 agrega una figura que va a jugar en el sorteo
 @param figuraId {number} id de la figura que sera ingresada
 @param premioId {number} id del premio al que pertenece la figura
 Nota: se asume que las figuras no se solapan y los premios no repiten figura
 **/
async function agregarFigura(figuraId, premioId) {

    let figuras = await db.collection('figuras').find({
        nm_pk: figuraId
    }).forEach(async function (figura) {
        figura.premioId = premioId;
        figura.ganado = false;
        //console.log(figura);
        let u = await db.collection('figura_juego').insertOne(figura);
    });

}

/**
 agrega un Lote a vendedidos
 @param cartonId {integer} id del carton
 Nota: se asume que los Lote vendidos no se solapan
 **/
async function agregarLote(cantidad) {
    await db.collection('cartones').find({
        tabla: {
            '$lte': cantidad
        }
    }).forEach(async function (doc) {
        //console.log(doc);
        await db.collection('vendidos').insertOne(doc); // start to replace
    })
}

/**
 * genera los tachados de una balota y retorna un objeto
 * con los ganadores o punteros segun sea el caso
 *
 *@param sorteo numero del sorteo
 *@param order orden de la balota
 *@param numBalota numero de la balota
 *@return objeto con los datos del parametro, los ganadores o punteros
 **/
async function siguienteBalota(sorteo, order, numBalota) {

    //let sorteo = 1;
    //let order = 1;
    //let numBalota = 1;
    //let nuevaBalotaObjeto = await nuevaBalota(sorteo, order, numBalota);

    let nuevaBalotaObjeto = await db.eval("siguienteBalota("+sorteo+", "+order+", "+numBalota+")");

    //let a = await db.collection('balota_sorteo').insertOne(nuevaBalotaObjeto);

    return nuevaBalotaObjeto;
}

/**
 *
 * @param sorteo
 * @param order
 * @param numBalota
 * @returns {{puntero: null, sorteo: *, balota: *, ganadores: null, order: *}}
 */
async function nuevaBalota(sorteo, order, numBalota) {

    let cantidad = 12;
    let listaGanadores;
    let objeto = {
        "sorteo": sorteo,
        "order": order,
        "balota": numBalota,
        "ganadores": null,
        "puntero": null

    };

    let u = await creaTachadosXBalota(numBalota, order);

    if (order <= cantidad) {//TODO Buscar ganadores solo de figura seleccionada.
        console.log("* * * hay menos de 24 !");
        return objeto;
    }
    //print("hay mas de 24");
    listaGanadores = await buscaGanadores(sorteo);

    //print(listaGanadores);

    let ganadoresListlength = listaGanadores.length;
    if ((listaGanadores) && (ganadoresListlength > 0)) {

        console.log("* * * hay ganadores !");
        console.log(listaGanadores);

        let gana = [];

        for (let i = 0; i < ganadoresListlength; i++) {
            let ganad = listaGanadores[i];
            //print(ganad);
            //print(ganad._id);
            //print(ganad._id.tabla);
            gana.push(ganad._id.tabla);
        }

        objeto.ganadores = gana;

        return objeto;
    }

    //print("no hay ganadores");
    let listaPunteros = await buscaPunteros(order, sorteo);
    //print(listaPunteros);
    let tablaPuntero = null;
    listaPunteros.forEach(function (puntero) {
        //print(puntero);
        //print(puntero._id);
        //print(puntero._id.tabla);
        tablaPuntero = puntero._id.tabla;
    });

    objeto.puntero = tablaPuntero;
    //print(objeto);

    return objeto;
}


/**
 llena la tabla de tachados por la balota que esta en juego
 @param numBalota    {integer} numero de la balota
 @param orden        {integer} orden de la balota

 **/
async function creaTachadosXBalota(numBalota, orden) {

    console.log('numBalota', numBalota, 'orden', orden);
    let tachados = [];

    let u = await db.collection('vendidos').aggregate([{
        $match: { //busca cartones por numero
            "numero": numBalota
        }
    }, {
        $group: { // agrupa por posicion
            _id: "$posicion",
            tablas: {
                $addToSet: "$tabla"
            }
        }
    }], async function (err, cursor) {
        assert.equal(err, null);

        cursor.toArray(async function (err, grupos) { //por cada grupo de posiciones
            //console.log("grupo",grupos);

            grupos.forEach(async function (grupo) {
                let posicion = grupo._id;
                let tablas = grupo.tablas;
                //console.log("posicion", posicion, 'tablas', tablas);
                let figura_juegos = await db.collection('figura_juego')
                    .find({
                        posiciones_nm_pk: posicion,
                        ganado: false
                    }).forEach(async function (figura_juego) {
                        //console.log("figura_juego",figura_juego);

                        tablas.forEach(async function (tabla) { //por cada tabla
                            //console.log('tabla', tabla, 'figura_juego',figura_juego);
                            //let tachados = figura_juego.tachados;
                            let casillas = figura_juego.nm_casillas;
                            let idFigura = figura_juego.nm_pk;
                            let idPremio = figura_juego.premioId;

                            if (idFigura) {
                                let nuevoTachado = {
                                    idFigura: idFigura,
                                    tabla: tabla,
                                    posicion: posicion,
                                    casillas: casillas,
                                    idPremio: idPremio,
                                    order: orden
                                };

                                //console.log("nuevoTachado", nuevoTachado);
                                //console.log("tachados",tachados);
                                //tachados.push(nuevoTachado);
                                //console.log("tachados",tachados);
                                let u = await db.collection('tachados').insertOne(nuevoTachado);
                            }

                        });
                    });

            }); //busca todas las figuras con esa posicion


        });
    });
    /*console.log("tachados",tachados.length ,tachados);
    if(tachados.length > 0){
        let a = await db.collection('tachados').insertMany(tachados);
    }*/


}


/**
 * Busca si hay ganadores de un sorteo
 * @param sorteo numero del sorteo
 **/
async function buscaGanadores(sorteo) {
    let ganadores = [];

    async function inResult(err, cursor) {
        assert.equal(err, null);
        //console.log("-----", cursor);

        await cursor.forEach(function (doc) {
            if (doc) {
                //console.log("doc ganador", doc);
                let idFigura = doc._id.idFigura;
                //console.log("ganador",doc);
                limpiaFiguraGanada(idFigura);
                ganadores.push(doc);
            }

        });
        //console.log("ganadores array", ganadores);
        if (ganadores.length > 0) {
            db.collection('ganadores').insertMany(ganadores);
        }

    }

    let ganadoresPromesa = await db.collection('tachados').aggregate([
        //{ $match: { idFigura:32 } },
        {
            $group: {
                _id: {
                    tabla: "$tabla",
                    idFigura: "$idFigura"
                },
                cantidad: {
                    $sum: 1
                },
                fcasillas: {
                    $max: "$casillas"
                },
                idpremio: {
                    $first: "$idPremio"
                },
                order: {
                    $max: "$order"
                },
                numero: {
                    $last: "$numero"
                },
                tabla: {
                    $first: "$tabla"
                },
                sorteo: {
                    $first: "$sorteo"
                }
            }
        }, {
            $project: {
                _id: 1,
                cantidad: 1,
                fcasillas: 1,
                idpremio: 1,
                order: 1,
                numero: 1,
                sorteo: 1,
                isGanador: {
                    $eq: ['$fcasillas', '$cantidad']
                }
            }
        },
        /*{
        $sort: {"cantidad":-1}
        }*/
        {
            $match: {
                isGanador: true
            }
        }
    ], inResult);

    return ganadores;
}

/**
 *
 * @param idFigura
 */
async function limpiaFiguraGanada(idFigura) {
    //console.log('limpia figura: ' + idFigura);
    // limpia tachados con esa figura
    await db.collection('tachados').deleteMany({
        idFigura: idFigura
    });

    // actualiza letiable ganado en figuras_juego
    await db.collection('figura_juego').updateMany({
        nm_pk: idFigura
    }, {
        $set: {
            ganado: true
        }
    })

}

/**
 *
 * @param ordenactual orden en el que se va buscar el puntero
 * @param sorteo numero del sorteo
 */
async function buscaPunteros(ordenactual, sorteo) {
    let punteros = [];

    async function inResult(err, cursor) {
        assert.equal(err, null);
        //console.log("-----", cursor);

        await cursor.forEach(function (doc) {
            if (doc) {
                //      console.log("doc punteros", doc);
                let idFigura = doc._id.idFigura;
                console.log("punteros", doc);

                punteros.push(doc);
            }

        });
        //console.log("punteros array", punteros);
        if (punteros.length > 0) {
            //db.collection('punteros').insertMany(punteros);

        }

    }

    let punterosPromesa = await db.collection('tachados').aggregate([
        //{ $match: { order:5 } },
        {
            $match: {
                order: {
                    $lt: ordenactual
                }
            }
        }, {
            $group: {
                _id: {
                    tabla: "$tabla",
                    idFigura: "$idFigura"
                },
                cantidad: {
                    $sum: 1
                },
                fcasillas: {
                    $max: "$casillas"
                },
                idpremio: {
                    $first: "$idPremio"
                },
                order: {
                    $max: "$order"
                },
                numero: {
                    $last: "$numero"
                },
                tabla: {
                    $first: "$tabla"
                }
            }
        }, {
            $project: {
                _id: 1,
                cantidad: 1,
                fcasillas: 1,
                idpremio: 1,
                order: 1,
                numero: 1,
                sorteo: 1,
                isGanador: {
                    $eq: ['$fcasillas', '$cantidad']
                }
            }
        }, {
            $sort: {
                "cantidad": -1
            }
        }, {
            $limit: 1
        }
    ], inResult);


    return punteros;
}