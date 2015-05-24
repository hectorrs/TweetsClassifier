import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MapReduceCommand;
import com.mongodb.MapReduceOutput;
import com.mongodb.MongoClient;
import com.mongodb.util.JSON;

public class Twitter {
	// Creación de variables privadas necesarias para la conexión a MongoDB
	private MongoClient mongo;
	private DB bd;
	
	// Constructor que permite la conexión con MongoDB. Necesita el usuario al que conectarse y el puerto utilizado
	public Twitter(String usuario, int puerto){
		try{
			this.mongo = new MongoClient(usuario, puerto);
		}catch(Exception e){
			System.out.println("Usuario o puerto incorrectos.");
		}
	}
	
	// Establece una conexión a la base de datos
	public void conexion(String nombre){
		this.bd = mongo.getDB(nombre);
	}
	
	// Selecciona una colección; si no está creada, la crea
	public DBCollection getColeccion(String nombre){
		return this.bd.getCollection(nombre);
	}
	
	// Crea una localidad
	public DBObject crearLocalidad(String ciudad, String pais){
		BasicDBObject localidad = new BasicDBObject();
		
		localidad.put("ciudad", ciudad);
		localidad.put("pais", pais);
		
		return localidad;
	}
	
	// Inserta un usuario en la colección
	public void insertarUsuario(DBCollection coleccion, String id, String email, String nombre, String fechaIngreso, DBObject localidad){
		BasicDBObject documento = new BasicDBObject();
		
		documento.put("_id", id);
		documento.put("email", email);
		documento.put("nombre", nombre);
		documento.put("fechaIngreso", fechaIngreso);
		documento.put("localidad", localidad);
		
		coleccion.insert(documento);
	}
	
	// Visualiza una colección
	public void verColeccion(DBCollection coleccion){
		DBCursor cursorDoc = coleccion.find();
		
		while (cursorDoc.hasNext()) {
			System.out.println(cursorDoc.next());
		}
	}
	
	// Inserta un twit en la colección
	public void insertarTwit(DBCollection coleccion, String id, String texto, String fechaCreacion, int numRt, int numFv, String usuario){
		BasicDBObject documento = new BasicDBObject();
		
		documento.put("_id", id);
		documento.put("texto", texto);
		documento.put("fechaCreacion", fechaCreacion);
		documento.put("numRt", numRt);
		documento.put("numFv", numFv);
		documento.put("usuario", usuario);
		
		coleccion.insert(documento);
	}
	
	// Devuelve el número de twits de la colección
	public int numTwits(DBCollection coleccion){
		return (int) coleccion.count();
	}
	
	// Visualiza los usuarios que viven en una ciudad de un país
	public void usuariosCiudad(DBCollection coleccion, String ciudad, String pais){
		BasicDBObject localidad = new BasicDBObject();
		localidad.put("ciudad", ciudad);
		localidad.put("pais", pais);
		
		BasicDBObject documento = new BasicDBObject();
		documento.put("localidad", localidad);
		
		DBCursor cursor = coleccion.find(documento);
		while(cursor.hasNext()){
		    System.out.println("\t" + cursor.next().get("nombre"));
		}
	}
	
	// Visualiza los twits que tienen más de x RTs
	public void twitsRtVeces(DBCollection coleccion, int num){
		BasicDBObject documento = new BasicDBObject();
		documento.put("numRt", new BasicDBObject("$gt", 8));
		
		DBCursor cursor = coleccion.find(documento);
		while(cursor.hasNext()){
			System.out.println("\t" + cursor.next().get("texto"));
		}
	}
	
	// Visualiza los twits que tienen la palabra 'palabra'
	public void twitsConPalabra(DBCollection coleccion, String palabra){
		BasicDBObject documento = new BasicDBObject();
		documento.put("texto", new BasicDBObject("$regex", palabra));
		
		DBCursor cursor = coleccion.find(documento);
		while (cursor.hasNext()){
			System.out.println("\t" + cursor.next().get("texto"));
		}
	}
	
	// Visualiza los twits de los usuarios que viven en la ciudad x 
	public void twitsUsuarioCiudad(DBCollection coleccion1, DBCollection coleccion2, String ciudad){
		DBCursor usuarios = coleccion1.find();
		DBCursor twits = coleccion2.find();
		
		while(usuarios.hasNext()){
			DBObject usuario = usuarios.next();
			String idUsuario = (String) usuario.get("_id");
			DBObject localidad = (DBObject) usuario.get("localidad");
			String ciudadUsuario = (String) localidad.get("ciudad");
			
			if(ciudadUsuario.equals(ciudad)){
				while(twits.hasNext()){
					DBObject twit = twits.next();
					if(twit.get("usuario").equals(idUsuario)){
						System.out.println("\t" + twit.get("texto"));
					}
				}
			}
		}
	}
	
	// Visualiza los twits de los usuarios que viven en el país x de forma decreciente
	public void twitsUsuariosPaisDesc(DBCollection coleccion1, DBCollection coleccion2, DBCollection coleccion3, String pais){
		DBCursor usuarios = coleccion1.find();
		DBCursor twits = coleccion2.find();
		
		while(usuarios.hasNext()){
			DBObject usuario = usuarios.next();
			String idUsuario = (String) usuario.get("_id");
			DBObject localidad = (DBObject) usuario.get("localidad");
			String paisUsuario = (String) localidad.get("pais");
			
			if(paisUsuario.equals(pais)){
				while(twits.hasNext()){
					DBObject twit = twits.next();
					
					if(twit.get("usuario").equals(idUsuario)){
						coleccion3.insert(twit);
					}
				}
			}
		}
		
		DBCursor ordenar = coleccion3.find();
		ordenar.sort(new BasicDBObject("numFV", 1));
		while(ordenar.hasNext()){
			DBObject twit = ordenar.next();
			System.out.println("\t" + twit.get("texto") + "\tNumFv: " + twit.get("numFv"));
		}
	}
	
	// Carga el fichero de texto
	@SuppressWarnings("resource")
	public void cargarFichero(DBCollection coleccion){
		try{
			File archivo = new File("corpusTwitter.txt");
			FileReader fr = new FileReader(archivo);
			BufferedReader br = new BufferedReader(fr);
			String json = br.readLine();
			
			while((json = br.readLine()) != null){
				DBObject dbObject = (DBObject)JSON.parse(json);
				coleccion.insert(dbObject);
			}
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	
	// Devuelve un map reduce de los twits
	public MapReduceOutput getMapReduce(DBCollection coleccion){
		String map = "function() { "
				+ "var words = this.text.toString().split(' ');"
				+ "for (var index = 0; index < words.length; ++index) {emit(this.id+'_@_'+this.category+'_@_'+ words[index], 1);}"
				+ "}";

		String reduce = " function(key,values){ " 
				+ " return Array.sum(values); "
				+ " }; ";
		
		MapReduceCommand cmd = new MapReduceCommand(coleccion, map, reduce, null, MapReduceCommand.OutputType.INLINE, null);
		
		MapReduceOutput out = coleccion.mapReduce(cmd);
		
		return out;
	}
	
	// Aplica la función stem a los twits guardados en el map reduce
	@SuppressWarnings("unchecked")
	public Map<String, Double> stem(MapReduceOutput out){
		Map<String, Double> map = new HashMap<String, Double>();
		Map<String, Double> aux = new HashMap<String, Double>();
		WordStemmer ws = new WordStemmer();

		for (DBObject o : out.results()) {
			aux = o.toMap();
			
			String[] palabras = o.toString().split("_@_");
			String[] palabras2 = palabras[2].split(",");
			String palabra = ws.stem(palabras2[0]);
			double valor = aux.get("value");
			
			map.put(palabras[0] + "_@_" + palabras[1] + "_@_"+ palabra, valor);
			aux.clear();
		}
		
		return map;
	}
	
	// Obtiene el TF
	public static Double getTF(String clave, Map<String, Double> map) {
		String[] palabras = clave.split("_@_");
		Double cont = 0.0;
		
		for (Map.Entry<String, Double> m : map.entrySet()) {
			String[] datos = m.getKey().split("_@_");
			
			if (datos[0].equalsIgnoreCase(palabras[0])) {
				cont++;
			}
		}
		return map.get(clave) / cont;
	}
	
	// Comprueba si un elemento está o no en una lista
	public static boolean estaElem(String elem, List<String> lista){
		for(int i = 0; i < lista.size(); i++){
			if(lista.get(i).equalsIgnoreCase(elem))
				return true;
		}
		return false;
	}
	
	// Obtiene el IDF
	public static Double getIDF(String clave, Map<String, Double> map, DBCollection col) {
		String[] palabras = clave.split("_@_");
		List<String> lista = new ArrayList<String>();
		
		for (Map.Entry<String, Double> m : map.entrySet()) {
			String[] texto = m.getKey().split("_@_");
			
			if (texto[2].equals(palabras[2])) {
				if (!estaElem(texto[0], lista)) {
					lista.add(texto[0]);
				}
			}
		}
		
		int size = (int) col.count();
		double tam = size;
		
		return Math.log(tam/lista.size());
	}
	
	// Obtiene el TF-IDF y devuelve el mapa con el resultado final
	public HashMap<String, Double> tf_idf(Map<String, Double> map, DBCollection col){
		HashMap<String, Double> mapFinal = new HashMap<String, Double>();
		
		for (Map.Entry<String, Double> v : map.entrySet()) {
			double tf_idf= getTF(v.getKey(), map) * getIDF(v.getKey(), map, col);
			mapFinal.put(v.getKey(), tf_idf);
		}
		
		return mapFinal;
	}
}