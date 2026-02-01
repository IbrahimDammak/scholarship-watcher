/**
 * Scholarship Watcher - Subscription Form Script
 * 
 * Handles:
 * - Dynamic country loading from configuration
 * - Form validation
 * - Netlify Forms submission
 * - UI state management
 */

(function() {
    'use strict';

    // Configuration
    const CONFIG = {
        countriesUrl: './countries.json',
        minCountries: 1
    };

    // DOM Elements
    const elements = {
        form: document.getElementById('subscription-form'),
        emailInput: document.getElementById('email'),
        emailError: document.getElementById('email-error'),
        countriesContainer: document.getElementById('countries-container'),
        countriesError: document.getElementById('countries-error'),
        submitBtn: document.getElementById('submit-btn'),
        btnText: document.querySelector('.btn-text'),
        btnLoading: document.querySelector('.btn-loading'),
        selectAll: document.getElementById('select-all'),
        selectNone: document.getElementById('select-none'),
        successMessage: document.getElementById('success-message'),
        errorMessage: document.getElementById('error-message'),
        errorDetails: document.getElementById('error-details'),
        subscribeAnother: document.getElementById('subscribe-another'),
        tryAgain: document.getElementById('try-again')
    };

    // Country flag emoji mapping
    const FLAGS = {
        'NO': '🇳🇴', 'SE': '🇸🇪', 'DK': '🇩🇰', 'FI': '🇫🇮',
        'DE': '🇩🇪', 'NL': '🇳🇱', 'BE': '🇧🇪', 'LU': '🇱🇺',
        'FR': '🇫🇷', 'CH': '🇨🇭', 'AT': '🇦🇹', 'IT': '🇮🇹',
        'ES': '🇪🇸', 'PT': '🇵🇹', 'GR': '🇬🇷', 'MT': '🇲🇹',
        'CY': '🇨🇾', 'PL': '🇵🇱', 'CZ': '🇨🇿', 'HU': '🇭🇺',
        'SK': '🇸🇰', 'SI': '🇸🇮', 'EE': '🇪🇪', 'LV': '🇱🇻',
        'LT': '🇱🇹', 'RO': '🇷🇴', 'BG': '🇧🇬', 'HR': '🇭🇷',
        'IE': '🇮🇪', 'EU': '🇪🇺'
    };

    // State
    let countries = [];

    /**
     * Initialize the application
     */
    async function init() {
        try {
            await loadCountries();
            setupEventListeners();
        } catch (error) {
            console.error('Initialization failed:', error);
            showCountriesError('Failed to load countries. Please refresh the page.');
        }
    }

    /**
     * Load countries from configuration file
     */
    async function loadCountries() {
        const response = await fetch(CONFIG.countriesUrl);
        
        if (!response.ok) {
            throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }

        const data = await response.json();
        
        if (!data.countries || !Array.isArray(data.countries)) {
            throw new Error('Invalid countries data format');
        }

        // Filter enabled countries and sort by name
        countries = data.countries
            .filter(c => c.enabled !== false)
            .sort((a, b) => a.name.localeCompare(b.name));

        renderCountries();
    }

    /**
     * Render country checkboxes
     */
    function renderCountries() {
        if (countries.length === 0) {
            elements.countriesContainer.innerHTML = '<p class="loading">No countries available</p>';
            return;
        }

        const html = countries.map(country => {
            const flag = FLAGS[country.code] || '🌍';
            return `
                <div class="country-item">
                    <input 
                        type="checkbox" 
                        id="country-${country.code}" 
                        name="country-checkbox"
                        value="${country.code}"
                        data-name="${country.name}"
                    >
                    <label for="country-${country.code}">
                        <span class="country-flag">${flag}</span>
                        <span class="country-name">${country.name}</span>
                    </label>
                </div>
            `;
        }).join('');

        elements.countriesContainer.innerHTML = html;
    }

    /**
     * Show error in countries container
     */
    function showCountriesError(message) {
        elements.countriesContainer.innerHTML = `<p class="loading" style="color: var(--color-error);">${message}</p>`;
    }

    /**
     * Setup event listeners
     */
    function setupEventListeners() {
        // Form submission
        elements.form.addEventListener('submit', handleSubmit);

        // Email validation on blur
        elements.emailInput.addEventListener('blur', validateEmail);
        elements.emailInput.addEventListener('input', clearEmailError);

        // Country selection
        elements.countriesContainer.addEventListener('change', handleCountryChange);

        // Select all / none buttons
        elements.selectAll.addEventListener('click', () => setAllCountries(true));
        elements.selectNone.addEventListener('click', () => setAllCountries(false));

        // Reset buttons
        elements.subscribeAnother.addEventListener('click', resetForm);
        elements.tryAgain.addEventListener('click', resetForm);
    }

    /**
     * Handle form submission
     */
    async function handleSubmit(event) {
        event.preventDefault();

        // Check honeypot (spam protection)
        const botField = document.getElementById('bot-field');
        if (botField && botField.value) {
            // Silently reject spam submissions
            showSuccess();
            return;
        }

        // Validate form
        const isEmailValid = validateEmail();
        const isCountriesValid = validateCountries();

        if (!isEmailValid || !isCountriesValid) {
            return;
        }

        // Prepare data
        const selectedCountries = getSelectedCountries();

        // Show loading state
        setSubmitLoading(true);

        try {
            // Submit to Netlify Function
            const payload = {
                email: elements.emailInput.value.trim(),
                countries: selectedCountries,
                created_at: new Date().toISOString()
            };
            
            const response = await fetch('/.netlify/functions/subscribe', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload)
            });

            const result = await response.json().catch(() => ({}));

            if (response.ok) {
                showSuccess();
            } else {
                throw new Error(result.error || `Submission failed: ${response.status}`);
            }
        } catch (error) {
            console.error('Submission error:', error);
            showError(error.message || 'Failed to submit. Please try again.');
        } finally {
            setSubmitLoading(false);
        }
    }

    /**
     * Validate email input
     */
    function validateEmail() {
        const email = elements.emailInput.value.trim();
        const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;

        if (!email) {
            showEmailError('Email address is required');
            return false;
        }

        if (!emailRegex.test(email)) {
            showEmailError('Please enter a valid email address');
            return false;
        }

        clearEmailError();
        return true;
    }

    /**
     * Show email error
     */
    function showEmailError(message) {
        elements.emailError.textContent = message;
        elements.emailInput.classList.add('invalid');
    }

    /**
     * Clear email error
     */
    function clearEmailError() {
        elements.emailError.textContent = '';
        elements.emailInput.classList.remove('invalid');
    }

    /**
     * Validate country selection
     */
    function validateCountries() {
        const selected = getSelectedCountries();

        if (selected.length < CONFIG.minCountries) {
            elements.countriesError.textContent = 'Please select at least one country';
            return false;
        }

        elements.countriesError.textContent = '';
        return true;
    }

    /**
     * Handle country checkbox change
     */
    function handleCountryChange() {
        // Clear error when user selects a country
        if (getSelectedCountries().length >= CONFIG.minCountries) {
            elements.countriesError.textContent = '';
        }
    }

    /**
     * Get selected country codes
     */
    function getSelectedCountries() {
        const checkboxes = elements.countriesContainer.querySelectorAll('input[type="checkbox"]:checked');
        return Array.from(checkboxes).map(cb => cb.value);
    }

    /**
     * Select or deselect all countries
     */
    function setAllCountries(selected) {
        const checkboxes = elements.countriesContainer.querySelectorAll('input[type="checkbox"]');
        checkboxes.forEach(cb => cb.checked = selected);
        
        if (selected) {
            elements.countriesError.textContent = '';
        }
    }

    /**
     * Set submit button loading state
     */
    function setSubmitLoading(loading) {
        elements.submitBtn.disabled = loading;
        elements.btnText.classList.toggle('hidden', loading);
        elements.btnLoading.classList.toggle('hidden', !loading);
    }

    /**
     * Show success message
     */
    function showSuccess() {
        elements.form.classList.add('hidden');
        elements.errorMessage.classList.add('hidden');
        elements.successMessage.classList.remove('hidden');
    }

    /**
     * Show error message
     */
    function showError(message) {
        elements.form.classList.add('hidden');
        elements.successMessage.classList.add('hidden');
        elements.errorDetails.textContent = message;
        elements.errorMessage.classList.remove('hidden');
    }

    /**
     * Reset form to initial state
     */
    function resetForm() {
        elements.form.reset();
        elements.form.classList.remove('hidden');
        elements.successMessage.classList.add('hidden');
        elements.errorMessage.classList.add('hidden');
        clearEmailError();
        elements.countriesError.textContent = '';
        setAllCountries(false);
    }

    // Initialize when DOM is ready
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }
})();
