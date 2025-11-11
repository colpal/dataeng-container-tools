use serde::{Deserialize, Serialize};
use std::path::Path;
use std::process::Command;
use regex::Regex;

#[derive(Debug, Deserialize, Serialize)]
struct ArgparseArg {
    option_strings: Vec<String>,
    dest: String,
    help: Option<String>,
    #[serde(rename = "type")]
    arg_type: String,
    default: serde_json::Value,
    required: bool,
    nargs: serde_json::Value,
    choices: Option<serde_json::Value>,
    metavar: Option<String>,
}

const FILTERED_OPTIONS: &[&str] = &["--help", "--dump_argparse_schema"];

pub async fn generate_markdown(python_file: &Path, verbose: bool) -> Result<(), Box<dyn std::error::Error>> {
    if verbose {
        eprintln!("Running Python script: {:?}", python_file);
    }

    // Run the Python script with --dump_argparse_schema
    let output = Command::new("python3")
        .arg(python_file)
        .arg("--dump_argparse_schema")
        .output()
        .map_err(|e| format!("Failed to execute Python script: {}", e))?;

    // Parse only stdout as UTF-8
    let stdout = String::from_utf8(output.stdout)
        .map_err(|e| format!("Failed to parse stdout as UTF-8: {}", e))?;

    if verbose {
        eprintln!("Parsing argparse schema...");
    }

    // Use regex to find all JSON arrays in stdout
    let re = Regex::new(r"\[\s*\{[\s\S]*?\}\s*\]").unwrap();
    let last_json = re.find_iter(&stdout).last()
        .ok_or("No JSON array found in stdout")?
        .as_str();

    let args: Vec<ArgparseArg> = serde_json::from_str(last_json)
        .map_err(|e| format!("Failed to parse JSON schema: {}", e))?;

    // Filter out unwanted options
    let filtered_args: Vec<ArgparseArg> = args
        .into_iter()
        .filter(|arg| {
            !arg.option_strings.iter().any(|opt| FILTERED_OPTIONS.contains(&opt.as_str()))
        })
        .collect();

    // Generate markdown table
    let markdown = generate_table(&filtered_args);

    println!("{}", markdown);

    Ok(())
}

fn generate_table(args: &[ArgparseArg]) -> String {
    let mut output = String::new();

    // Add header title
    output.push_str("## Command-Line Arguments Table\n\n");

    // Table header
    output.push_str("| Field | Type | Default | Required | Description |\n");
    output.push_str("|-------|------|---------|----------|-------------|\n");

    for arg in args {
        let field = arg.option_strings.join(", ");
        let arg_type = &arg.arg_type;
        let default = format_default(&arg.default);
        let required = if arg.required { "Yes" } else { "No" };
        let description = arg.help.as_deref().unwrap_or("").replace("|", "\\|");

        output.push_str(&format!(
            "| {} | {} | {} | {} | {} |\n",
            field, arg_type, default, required, description
        ));
    }

    output
}

fn format_default(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Null => "None".to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::String(s) => {
            if s == "==SUPPRESS==" {
                "-".to_string()
            } else {
                format!("`{}`", s)
            }
        }
        serde_json::Value::Array(arr) => format!("{:?}", arr),
        serde_json::Value::Object(_) => "{}".to_string(),
    }
}
