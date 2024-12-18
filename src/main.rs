// Sample functions with varying numbers of parameters
fn func1(param: i32) {
    println!("func1: Received integer: {}", param);
}

fn func2(param: &str) {
    println!("func2: Received string: {}", param);
}

fn func3(param: bool) {
    println!("func3: Received boolean: {}", param);
}

fn func4(param1: &str, param2: i32) {
    println!("func4: Received string: {} and integer: {}", param1, param2);
}

// Define the macro to handle a variable number of functions and parameters
macro_rules! run_etl {
    // Case where there are multiple function calls to handle
    ($($func:ident($($param:expr),*)),*) => {
        // Expanding the function calls one by one
        $(
            $func($($param),*);
        )*
    };
}

fn main() {
    // Example usage of the macro with different functions having different numbers of parameters
    run_etl!(
        func1(10),               // func1 with one parameter
        func2("Hello, world!"),  // func2 with one parameter
        func3(true),             // func3 with one parameter
        func4("Hello", 42)      // func4 with two parameters
    );
}
