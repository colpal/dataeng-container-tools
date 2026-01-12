use bollard::Docker;
use bollard::models::ContainerCreateBody;
use bollard::query_parameters::{CreateContainerOptions, RemoveContainerOptionsBuilder, AttachContainerOptionsBuilder, RemoveImageOptionsBuilder, StartContainerOptions};
use futures_util::stream::StreamExt;
use std::path::Path;
use tokio::io::AsyncWriteExt;
use tokio::task::spawn;

use crossterm::terminal::{disable_raw_mode, enable_raw_mode};
use std::io::{stdout, Write};
use tokio::io::{stdin, AsyncReadExt};

use super::{build_image, get_image_tag};

pub async fn test_container(path: &Path, bash: bool, verbose: bool) -> Result<(), Box<dyn std::error::Error>> {
    let docker = Docker::connect_with_local_defaults()?;
    
    // Build the image first
    build_image(path, verbose).await?;
    
    // Get the image tag that was built
    let image_tag = get_image_tag(path);
    
    println!("\nStarting container for testing...");
    
    // Configure container
    let mut config = ContainerCreateBody {
        image: Some(image_tag.clone()),
        tty: Some(true),
        attach_stdout: Some(true),
        attach_stderr: Some(true),
        open_stdin: Some(true),
        attach_stdin: Some(true),
        ..Default::default()
    };
    
    // Override with bash if requested
    if bash {
        config.entrypoint = Some(vec!["/bin/bash".to_string()]);
        config.cmd = Some(vec![]);
        println!("Interactive bash mode enabled");
    }
    
    let container = docker.create_container(None::<CreateContainerOptions>, config).await?;
    let container_id = container.id.clone();
    
    println!("✓ Container created: {}", container_id);
    
    // Cleanup function
    let cleanup = || async {
        // Ensure raw mode is disabled before printing cleanup messages
        let _ = disable_raw_mode();
        println!("\nCleaning up...");
        
        // Remove container
        let remove_options = RemoveContainerOptionsBuilder::default()
            .force(true)
            .build();
        if let Err(e) = docker.remove_container(&container_id, Some(remove_options)).await {
            eprintln!("Warning: Failed to remove container: {}", e);
        } else {
            println!("✓ Container removed");
        }
        
        // Remove image
        let remove_image_options = RemoveImageOptionsBuilder::default()
            .force(true)
            .build();
        if let Err(e) = docker.remove_image(&image_tag, Some(remove_image_options), None).await {
            eprintln!("Warning: Failed to remove image: {}", e);
        } else {
            println!("✓ Image removed");
        }
    };
    
    // Start and attach to container
    docker.start_container(&container_id, None::<StartContainerOptions>).await?;
    println!("✓ Container started");
    
    // Interactive mode with proper TTY handling
    let attach_options = AttachContainerOptionsBuilder::default()
        .stdout(true)
        .stderr(true)
        .stdin(true)
        .stream(true)
        .build();
    
    // Enable raw mode for interactive session
    enable_raw_mode()?;

    let bollard::container::AttachContainerResults {
        mut output,
        mut input,
    } = docker.attach_container(&container_id, Some(attach_options)).await?;
    
    // Pipe stdin into the docker attach stream input
    spawn(async move {
        let mut stdin = stdin();
        let mut buf = [0u8; 32];
        loop {
            match stdin.read(&mut buf).await {
                Ok(0) => break, // EOF
                Ok(n) => {
                    if input.write_all(&buf[..n]).await.is_err() {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
    });
    
    // Pipe docker attach output into stdout
    let mut stdout = stdout();
    while let Some(Ok(output)) = output.next().await {
        stdout.write_all(output.into_bytes().as_ref())?;
        stdout.flush()?;
    }
    
    // Disable raw mode
    disable_raw_mode()?;
    
    // Cleanup
    cleanup().await;
    
    Ok(())
}
