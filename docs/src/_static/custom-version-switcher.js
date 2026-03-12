// Update version switcher button text to show current version instead of "Choose version"
document.addEventListener('DOMContentLoaded', function() {
    // Wait a bit for the version switcher to be populated by pydata-sphinx-theme
    setTimeout(function() {
        const versionButton = document.querySelector('.version-switcher__button');
        if (versionButton) {
            // Get the current version from the page context
            const currentVersion = DOCUMENTATION_OPTIONS.VERSION || 'latest';
            
            // For tagged releases (starting with 'v'), use actual version
            // For dev/latest builds, use placeholder string
            const displayVersion = currentVersion.startsWith('v') ? currentVersion : 'latest';
            
            // Update button text to show current version
            const buttonText = versionButton.childNodes[0];
            if (buttonText && buttonText.nodeType === Node.TEXT_NODE) {
                buttonText.textContent = displayVersion + ' ';
            }
        }
    }, 100);
});
