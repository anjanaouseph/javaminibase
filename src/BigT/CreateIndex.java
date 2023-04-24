package BigT;

public class CreateIndex {
    public CreateIndex(String btname, int type){

        try{
            bigt table = new bigt(btname);



        }catch (Exception exception) {
            System.out.println("Ran into exception while running createIndex");
            exception.printStackTrace();
        }

    }
}
