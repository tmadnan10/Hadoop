public class Thonga {
	public static void main(String[] args) {
		System.out.println("Argc: "+args.length);
		System.out.println("Argv: "+args[args.length-1]);
		String line;
		try (
		    InputStream fis = new FileInputStream("the_file_name");
		    InputStreamReader isr = new InputStreamReader(fis, Charset.forName("UTF-8"));
		    BufferedReader br = new BufferedReader(isr);
		) 

		{
		    while ((line = br.readLine()) != null) {
		        // Deal with the line
		    }
		}
			}
}