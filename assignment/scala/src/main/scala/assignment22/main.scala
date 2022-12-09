import assignment22.Assignment

object Main extends App {

    val a: Assignment = new Assignment()
    
    /* Task 1 */
    println("-----------TASK 1------------")

    println(a.task1(a.dataD2, 3).foreach(println))   

    /* Task 2 */
    println("-----------TASK 2------------")

    println(a.task2(a.dataD3, 4).foreach(println))

    /* Task 3 */
    println("-----------TASK 3------------")

    println(a.task3(a.dataD2WithLabels, 5).foreach(println))

    /* Task 3 */
    println("-----------TASK 4------------")

    println(a.task4(a.dataD2, 2, 13).foreach(println))
}