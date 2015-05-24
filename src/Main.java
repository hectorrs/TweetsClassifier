import java.util.HashMap;
import java.util.Map;

import com.mongodb.DBCollection;
import com.mongodb.MapReduceOutput;

public class Main {
	@SuppressWarnings("static-access")
	public static void main(String [] args){
		// Se usa un cronómetro para controlar el tiempo que tarda en ejecutarse la aplicación
		Cronometro c = new Cronometro();
		c.start();
		
		// Conexión con el server
		Twitter twitter = new Twitter("localhost", 27017);
		
		// Conexión con la base de datos
		twitter.conexion("twitter");
		
		// Creación o selección de las colecciones
		DBCollection usuarios = twitter.getColeccion("usuario");
		DBCollection twits = twitter.getColeccion("twit");
		DBCollection ordenados = twitter.getColeccion("ordenados");
		
		// Para evitar duplicados, previamente a la carga del archivo se borra el contenido de la colección
		usuarios.drop();
		twits.drop();
		ordenados.drop();
		
		// Inserción de los usuarios
		twitter.insertarUsuario(usuarios, "usuario0", "ana@gmail.com", "Ana", "01/04/2015", twitter.crearLocalidad("Ourense", "España"));
		twitter.insertarUsuario(usuarios, "usuario1", "pepito@gmail.com", "Pepito", "06/03/2015", twitter.crearLocalidad("Madrid", "España"));
		twitter.insertarUsuario(usuarios, "usuario2", "maria@gmail.com", "María", "02/09/2014", twitter.crearLocalidad("Vigo", "España"));
		twitter.insertarUsuario(usuarios, "usuario3", "paco@gmail.com", "Paco", "26/12/2014", twitter.crearLocalidad("Alicante", "España"));
		twitter.insertarUsuario(usuarios, "usuario4", "sonia@gmail.com", "Sonia", "24/04/2014", twitter.crearLocalidad("Barcelona", "España"));
		twitter.insertarUsuario(usuarios, "usuario5", "manuel@gmail.com", "Manuel", "21/12/2013", twitter.crearLocalidad("Valencia", "España"));
		twitter.insertarUsuario(usuarios, "usuario6", "elena@gmail.com", "Elena", "15/08/2014", twitter.crearLocalidad("Ourense", "España"));
		twitter.insertarUsuario(usuarios, "usuario7", "isma@gmail.com", "Ismael", "30/09/2014", twitter.crearLocalidad("Vigo", "España"));
		twitter.insertarUsuario(usuarios, "usuario8", "sergio@gmail.com", "Sergio", "26/12/2014", twitter.crearLocalidad("Málaga", "España"));
		twitter.insertarUsuario(usuarios, "usuario9", "raquel@gmail.com", "Raquel", "03/04/2014", twitter.crearLocalidad("Ourense", "España"));
		
		// Inserción de los twits
		twitter.insertarTwit(twits, "twit0", "Mi primer twit!!", "02/04/2015", 2, 4, "usuario0");
		twitter.insertarTwit(twits, "twit1", "MongoDB is cool ;)", "03/04/2015", 1, 2, "usuario0");
		twitter.insertarTwit(twits, "twit2", "Yupi :D", "26/04/2015", 7, 4, "usuario1");
		twitter.insertarTwit(twits, "twit3", "Hace frío...", "28/04/2015", 0, 0, "usuario1");
		twitter.insertarTwit(twits, "twit4", "Vacaciones :D :D", "02/09/2014", 10, 3, "usuario2");
		twitter.insertarTwit(twits, "twit5", "La quiero ya! jumm", "02/10/2014", 1, 3, "usuario2");
		twitter.insertarTwit(twits, "twit6", "Tonight...mmm...", "02/01/2015", 12, 8, "usuario3");
		twitter.insertarTwit(twits, "twit7", "Hello world!", "28/04/2015", 1, 0, "usuario4");
		twitter.insertarTwit(twits, "twit8", "Buenas noches", "02/10/2014", 2, 0, "usuario6");
		twitter.insertarTwit(twits, "twit9", "La lala lalala lala la", "5/04/2014", 20, 30, "usuario9");
		
		System.out.println("------------------------------------------------------------------------------- Usuarios -------------------------------------------------------------------------------");
		twitter.verColeccion(usuarios);
		
		System.out.println("\n-------------------------------------------------------------------- Twits --------------------------------------------------------------------");
		twitter.verColeccion(twits);
		
		System.out.println("\nHay " + twitter.numTwits(twits) + " twits.");
		
		System.out.println("\nLos siguientes usuarios viven en Ourense: ");
		twitter.usuariosCiudad(usuarios, "Ourense", "España");
		
		System.out.println("\nLos siguientes twits tienen más de 8 Rt:");
		twitter.twitsRtVeces(twits, 8);
		
		System.out.println("\nLos siguientes twits tienen la palabra 'La':");
		twitter.twitsConPalabra(twits, "La");
		
		System.out.println("\nLos usuarios de los siguientes twits viven en Vigo:");
		twitter.twitsUsuarioCiudad(usuarios, twits, "Vigo");
		
		System.out.println("\nLos usuarios de los siguientes twits viven en España:");
		twitter.twitsUsuariosPaisDesc(usuarios, twits, ordenados, "España");
		
		// Creación o selección de la colección
		DBCollection twits2 = twitter.getColeccion("twit2");
		
		// Para evitar duplicados, previamente a la carga del archivo se borra el contenido de la colección
		twits2.drop();
		
		// Carga el fichero
		twitter.cargarFichero(twits2);
		
		// Generación de un map reduce, donde se extrae la frecuencia de cada palabra de todos los tweets
		System.out.println("\nGenerando Map Reduce...");
		MapReduceOutput out = twitter.getMapReduce(twits2);
		
		// Mediante la realización de un proceso de "stem", se unifican y eliminan las palabras y sus frecuencias que no sean relevantes
		System.out.println("\nRealizando Stem...");
		Map<String, Double> map = twitter.stem(out);
		
		// Se calcula el TF-IDF, es decir, una medida estadística que se utiliza para evaluar la importancia de una palabra en un documento (corpus)
		System.out.println("\nCalculando TF-IDF...");
		HashMap<String, Double> mapFinal = twitter.tf_idf(map, twits2);
		
		// Generación de un archivo .arff para la posteración evaluación en el programa Weka
		System.out.println("\nGenerando archivo .arff...");
		InstanceBuilder ib = new InstanceBuilder();
		ib.convertToInstances(mapFinal, "Twitter.arff");
		
		System.out.println("\nArchivo generado :D\n");
		
		// Finalización del cronómetro
		c.interrupt();
	}
}