const fs = require('fs');
const path = require('path');

exports.handler = async (event, context) => {
    try {
        const configPath = path.join(__dirname, '..', '..', 'config', 'countries.json');
        
        let countriesData;
        
        if (fs.existsSync(configPath)) {
            const rawData = fs.readFileSync(configPath, 'utf8');
            countriesData = JSON.parse(rawData);
        } else {
            countriesData = getDefaultCountries();
        }

        const enabledCountries = {
            countries: countriesData.countries
                .filter(c => c.enabled !== false)
                .map(c => ({
                    code: c.code,
                    name: c.name,
                    enabled: true
                }))
                .sort((a, b) => a.name.localeCompare(b.name))
        };

        return {
            statusCode: 200,
            headers: {
                'Content-Type': 'application/json',
                'Cache-Control': 'public, max-age=3600',
                'Access-Control-Allow-Origin': '*'
            },
            body: JSON.stringify(enabledCountries)
        };
    } catch (error) {
        console.error('Error loading countries:', error);
        
        return {
            statusCode: 500,
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({ error: 'Failed to load countries' })
        };
    }
};

function getDefaultCountries() {
    return {
        countries: [
            { code: 'NO', name: 'Norway', enabled: true },
            { code: 'SE', name: 'Sweden', enabled: true },
            { code: 'DE', name: 'Germany', enabled: true },
            { code: 'NL', name: 'Netherlands', enabled: true },
            { code: 'DK', name: 'Denmark', enabled: true },
            { code: 'FI', name: 'Finland', enabled: true },
            { code: 'FR', name: 'France', enabled: true },
            { code: 'BE', name: 'Belgium', enabled: true },
            { code: 'AT', name: 'Austria', enabled: true },
            { code: 'CH', name: 'Switzerland', enabled: true },
            { code: 'IT', name: 'Italy', enabled: true },
            { code: 'ES', name: 'Spain', enabled: true },
            { code: 'PT', name: 'Portugal', enabled: true },
            { code: 'IE', name: 'Ireland', enabled: true },
            { code: 'PL', name: 'Poland', enabled: true },
            { code: 'CZ', name: 'Czech Republic', enabled: true },
            { code: 'HU', name: 'Hungary', enabled: true },
            { code: 'EU', name: 'European Union', enabled: true }
        ]
    };
}
